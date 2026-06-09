import django, json, secrets, asyncio, os
from time import sleep, monotonic
from asgiref.sync import sync_to_async
from django.db.models import Sum, F, Q, Case, When, Value, IntegerField
from datetime import datetime, timedelta
from django.utils import timezone
from gui.lnd_deps import lightning_pb2 as ln
from gui.lnd_deps import lightning_pb2_grpc as lnrpc
from gui.lnd_deps import router_pb2 as lnr
from gui.lnd_deps import router_pb2_grpc as lnrouter
from gui.lnd_deps.lnd_connect import (
    get_shared_channel,
    get_shared_async_channel,
    close_shared_channel,
    close_shared_async_channel,
)
from os import environ
from typing import List

environ['DJANGO_SETTINGS_MODULE'] = 'lndg.settings'
django.setup()
from gui.models import (
    Rebalancer,
    Channels,
    LocalSettings,
    AllowedTarget,
    Forwards,
    Autopilot,
    RebalanceRoute,
    NodeReputation,
    calc_success_ratio,
    calc_weighted_ratio,
)
from utils import get_local_setting

# Per-source rebalance coordination (shared across workers)
# Failed-edge cache: (prev_pubkey, hop_pubkey) -> (monotonic_ts, amount_msat)
# Tracks TEMPORARY_CHANNEL_FAILUREs at the directed edge between two adjacent
# hop nodes for the amount that was being forwarded at the time. Matches
# regolancer's mcCache (mission_ctl.go). A candidate route is invalidated only
# if it actually re-uses one of these edges at a similar amount, so a failure
# between, say, our peer and node X doesn't block routes through node Y.
_failed_edges = {}
_FAILED_EDGE_EXPIRY = 600  # 10 minutes
_FAILED_EDGE_TOLERANCE_PPM = 50_000  # 5% — match regolancer's FailTolerance default

# GetChanInfo cache for max_htlc_msat lookups: chan_id -> (monotonic_ts, info)
_chan_info_cache = {}
_CHAN_INFO_TTL = 300  # seconds

# Cached identity pubkey, lazily fetched on first use.
_self_pubkey = None


def _get_self_pubkey(stub):
    global _self_pubkey
    if _self_pubkey is None:
        _self_pubkey = stub.GetInfo(ln.GetInfoRequest()).identity_pubkey
    return _self_pubkey


def _max_amount_on_route_msat(stub, route):
    """Walk the route and return the smallest max_htlc_msat across hops, or 0 on error.

    For each hop, use the policy of the side *opposite* to hop.pub_key (the forwarder).
    Mirrors regolancer's maxAmountOnRoute (mission_ctl.go).
    """
    cap = None
    now = monotonic()
    for hop in route.hops:
        chan_id = int(hop.chan_id)
        cached = _chan_info_cache.get(chan_id)
        if cached and now - cached[0] < _CHAN_INFO_TTL:
            info = cached[1]
        else:
            try:
                info = stub.GetChanInfo(ln.ChanInfoRequest(chan_id=chan_id))
                _chan_info_cache[chan_id] = (now, info)
            except Exception:
                continue
        # Policy of the side forwarding to hop.pub_key
        policy = info.node1_policy if hop.pub_key == info.node2_pub else info.node2_policy
        mh = getattr(policy, 'max_htlc_msat', 0) or 0
        if mh > 0 and (cap is None or mh < cap):
            cap = mh
    return cap or 0


def _prune_failed_edges():
    now = monotonic()
    expired = [k for k, (ts, _) in _failed_edges.items() if now - ts > _FAILED_EDGE_EXPIRY]
    for k in expired:
        _failed_edges.pop(k, None)


def _mark_edge_failed(prev_pubkey, hop_pubkey, amount_msat):
    """Cache (prev → hop) as failed at amount_msat."""
    if not prev_pubkey or not hop_pubkey:
        return
    _failed_edges[(prev_pubkey, hop_pubkey)] = (monotonic(), int(amount_msat or 0))


def _record_route_failure(route, failure):
    """If `failure` is a TEMPORARY_CHANNEL_FAILURE, cache the failing edge.

    Mirrors regolancer payment.go:128-131: the failing edge is from the
    previous hop to the failure-source hop, and the recorded amount is what
    the previous hop was forwarding.
    """
    if route is None or failure is None:
        return
    code = getattr(failure, 'code', None)
    fsi = getattr(failure, 'failure_source_index', None)
    if code != 15 or fsi is None or fsi <= 0:
        return
    hops = list(route.hops)
    if fsi >= len(hops):
        return
    prev = hops[fsi - 1]
    failed = hops[fsi]
    _mark_edge_failed(prev.pub_key, failed.pub_key, getattr(prev, 'amt_to_forward_msat', 0))


def _validate_route(route):
    """Return True if no consecutive hop edge in `route` is cached as failed
    at a similar amount (within _FAILED_EDGE_TOLERANCE_PPM)."""
    _prune_failed_edges()
    if route is None or len(route.hops) < 2:
        return True
    for i in range(len(route.hops) - 1):
        key = (route.hops[i].pub_key, route.hops[i + 1].pub_key)
        cached = _failed_edges.get(key)
        if not cached:
            continue
        _, failed_amt_msat = cached
        cur_amt_msat = getattr(route.hops[i], 'amt_to_forward_msat', 0) or 0
        if cur_amt_msat <= 0 or failed_amt_msat <= 0:
            return False
        denom = max(failed_amt_msat, cur_amt_msat)
        diff_ppm = abs(failed_amt_msat - cur_amt_msat) * 1_000_000 // denom
        if diff_ppm <= _FAILED_EDGE_TOLERANCE_PPM:
            return False
    return True


# map standard failure codes and internal details to enum names for clearer logs
FAILURE_CODE_NAMES = {
    1: "INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS",
    2: "INCORRECT_PAYMENT_AMOUNT",
    3: "FINAL_INCORRECT_CLTV_EXPIRY",
    4: "FINAL_INCORRECT_HTLC_AMOUNT",
    5: "FINAL_EXPIRY_TOO_SOON",
    6: "INVALID_REALM",
    7: "EXPIRY_TOO_SOON",
    8: "INVALID_ONION_VERSION",
    9: "INVALID_ONION_HMAC",
    10: "INVALID_ONION_KEY",
    11: "AMOUNT_BELOW_MINIMUM",
    12: "FEE_INSUFFICIENT",
    13: "INCORRECT_CLTV_EXPIRY",
    14: "CHANNEL_DISABLED",
    15: "TEMPORARY_CHANNEL_FAILURE",
    16: "REQUIRED_NODE_FEATURE_MISSING",
    17: "REQUIRED_CHANNEL_FEATURE_MISSING",
    18: "UNKNOWN_NEXT_PEER",
    19: "TEMPORARY_NODE_FAILURE",
    20: "PERMANENT_NODE_FAILURE",
    21: "PERMANENT_CHANNEL_FAILURE",
    22: "EXPIRY_TOO_FAR",
    23: "MPP_TIMEOUT",
}

FAILURE_DETAIL_NAMES = {
    0: "UNKNOWN",
    1: "NO_DETAIL",
    2: "ONION_DECODE",
    3: "LINK_NOT_ELIGIBLE",
    4: "ON_CHAIN_TIMEOUT",
    5: "HTLC_EXCEEDS_MAX",
    6: "INSUFFICIENT_BALANCE",
    7: "INCOMPLETE_FORWARD",
    8: "HTLC_ADD_FAILED",
    9: "FORWARDS_DISABLED",
    10: "INVOICE_CANCELED",
    11: "INVOICE_UNDERPAID",
    12: "INVOICE_EXPIRY_TOO_SOON",
    13: "INVOICE_NOT_OPEN",
    14: "MPP_INVOICE_TIMEOUT",
    15: "ADDRESS_MISMATCH",
    16: "SET_TOTAL_MISMATCH",
    17: "SET_TOTAL_TOO_LOW",
    18: "SET_OVERPAID",
    19: "UNKNOWN_INVOICE",
    20: "INVALID_KEYSEND",
    21: "MPP_IN_PROGRESS",
    22: "CIRCULAR_ROUTE",
}

PROBE_STEPS = 5
MIN_PROBE_AMOUNT = 69420

async def probe_route_amount(routerstub, hop_keys, outgoing_chan_id, cltv_delta, original_amount):
    """Binary search for max routable amount using fake payments."""
    good = 0
    bad = original_amount
    for step in range(PROBE_STEPS):
        probe = (good + bad) // 2
        if probe < MIN_PROBE_AMOUNT or bad - good < max(good // 20, 1000):
            break
        try:
            build = await routerstub.BuildRoute(
                lnr.BuildRouteRequest(
                    outgoing_chan_id=int(outgoing_chan_id),
                    amt_msat=probe * 1000,
                    hop_pubkeys=hop_keys,
                    final_cltv_delta=cltv_delta,
                )
            )
        except Exception:
            break
        fake_hash = os.urandom(32)
        try:
            result = await routerstub.SendToRouteV2(
                lnr.SendToRouteRequest(payment_hash=fake_hash, route=build.route),
                timeout=30,
            )
        except Exception:
            break
        failure = getattr(result, "failure", None)
        if not failure:
            break
        code = getattr(failure, "code", None)
        if code == 1:      # INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS → can succeed
            good = probe
        elif code == 15:    # TEMPORARY_CHANNEL_FAILURE → too much
            bad = probe
        elif code == 12:    # FEE_INSUFFICIENT → retry (don't update bounds)
            continue
        else:
            break
    return good if good >= MIN_PROBE_AMOUNT else 0


async def try_single_source(
    stub, routerstub, rebalance, source_chan_id, source_fee_map,
    target_fee_rate, ar_max_cost, max_fee_rate, invoice_response,
    fee_limit_msat, timeout,
):
    """Attempt a rebalance via a single outbound source channel.

    Uses QueryRoutes (fee_limit = target*max_cost% - source_fee) → BuildRoute →
    opportunity-cost check → SendToRouteV2. Returns a dict on success:
        {'status': 2, 'fees_paid': sats, 'successful_out': chan_id,
         'successful_in': chan_id, 'payment_hash': hex, 'value': sats}
    Returns None on any failure (caller should try next source).
    """
    src_fee = source_fee_map.get(str(source_chan_id), source_fee_map.get(source_chan_id, 0))
    # Per-source budget: how many ppm of route fee we can afford
    per_source_route_ppm = int(target_fee_rate * (ar_max_cost / 100)) - src_fee
    per_source_route_ppm = min(per_source_route_ppm, max_fee_rate)
    if per_source_route_ppm <= 0:
        return None
    per_source_fee_limit_sat = int(per_source_route_ppm * rebalance.value / 1_000_000)
    if per_source_fee_limit_sat <= 0:
        return None
    src_alias = await sync_to_async(lambda: Channels.objects.filter(chan_id=str(source_chan_id)).values_list('alias', flat=True).first())() or "?"
    src_label = f"{source_chan_id} ({src_alias})"

    # QueryRoutes for a self-payment rebalance: destination is US, last hop is
    # the target peer (matches regolancer routes.go:102-110). LND then constructs
    # the loop-back via the AR-target channel so the route actually ends at us.
    self_pubkey = await sync_to_async(_get_self_pubkey)(stub)
    try:
        qr_response = await sync_to_async(stub.QueryRoutes)(
            ln.QueryRoutesRequest(
                pub_key=self_pubkey,
                amt=rebalance.value,
                outgoing_chan_ids=[int(source_chan_id)],
                last_hop_pubkey=bytes.fromhex(rebalance.last_hop_pubkey),
                fee_limit=ln.FeeLimit(fixed=per_source_fee_limit_sat),
                use_mission_control=True,
            )
        )
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : QueryRoutes via {src_label} failed: {e}")
        return None

    if not qr_response or not qr_response.routes:
        return None

    for query_route in qr_response.routes:
        if not query_route.hops:
            continue
        # Validate against the failed-edge cache: skip routes that reuse a
        # known-bad (prev → next) edge at a similar amount.
        if not _validate_route(query_route):
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Route via {src_label} skipped — reuses a cached failed edge")
            continue
        # Extract hop pubkeys for BuildRoute
        hop_keys = [bytes.fromhex(h.pub_key) for h in query_route.hops]
        if len(query_route.hops) >= 2:
            cltv_delta = query_route.hops[-1].expiry - query_route.hops[-2].expiry
        else:
            cltv_delta = 144

        # BuildRoute with payment_addr so we can actually send via this route
        try:
            build = await routerstub.BuildRoute(
                lnr.BuildRouteRequest(
                    outgoing_chan_id=int(source_chan_id),
                    amt_msat=rebalance.value * 1000,
                    hop_pubkeys=hop_keys,
                    final_cltv_delta=cltv_delta,
                    payment_addr=invoice_response.payment_addr,
                )
            )
        except Exception as e:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute via {src_label} failed: {e}")
            continue

        # Opportunity-cost check (uses total_fees_msat which already includes inbound discounts)
        allowed, route_fee_ppm, max_route_ppm = check_opportunity_cost(
            build.route.total_fees_msat, rebalance.value,
            source_chan_id, source_fee_map,
            target_fee_rate, ar_max_cost, max_fee_rate,
        )
        if not allowed:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Route via {src_label} rejected: route {route_fee_ppm} ppm > max {max_route_ppm} ppm (target {target_fee_rate}*{ar_max_cost}% - src {src_fee})")
            continue

        # Global fee_limit check (safety, in case the per-source budget is looser than rebalance.fee_limit)
        if build.route.total_fees_msat > fee_limit_msat:
            actual_ppm = int((build.route.total_fees_msat / (rebalance.value * 1000)) * 1000000)
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Route via {src_label} exceeds rebalance fee_limit ({actual_ppm} ppm > {int(fee_limit_msat/rebalance.value*1000)} ppm)")
            continue

        route_msg = build.route
        rebuilt_hex = route_msg.SerializeToString().hex()

        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Sending via {src_label}: {len(route_msg.hops)} hops, {route_fee_ppm} ppm route + {src_fee} ppm source")

        try:
            send_response = await routerstub.SendToRouteV2(
                lnr.SendToRouteRequest(
                    payment_hash=invoice_response.r_hash,
                    route=route_msg,
                ),
                timeout=timeout,
            )
        except Exception as e:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : SendToRouteV2 via {src_label} error: {e}")
            return None

        # HTLCStatus: 1 = SUCCEEDED
        if send_response.status == 1:
            fees_msat = getattr(send_response, "fee_msat", None)
            if not fees_msat:
                try:
                    fees_msat = send_response.route.total_fees_msat
                except Exception:
                    fees_msat = 0
            await update_route(rebalance.last_hop_pubkey, source_chan_id, rebuilt_hex, True)
            await update_node_reputations(rebuilt_hex, True)
            return {
                'status': 2,
                'fees_paid': fees_msat / 1000 if fees_msat else 0,
                'successful_out': send_response.route.hops[0].chan_id,
                'successful_in': send_response.route.hops[-1].chan_id,
                'payment_hash': invoice_response.r_hash.hex(),
                'value': rebalance.value,
            }

        # Failure handling
        failure = getattr(send_response, "failure", None)
        code_num = getattr(failure, "code", None) if failure else None
        fsi = getattr(failure, "failure_source_index", None) if failure else None
        reason = FAILURE_CODE_NAMES.get(code_num, code_num)
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Route via {src_label} failed: code={reason} hop={fsi}")
        _record_route_failure(route_msg, failure)
        await update_route(rebalance.last_hop_pubkey, source_chan_id, rebuilt_hex, False)
        await update_node_reputations(rebuilt_hex, False, fsi)

        # TEMPORARY_CHANNEL_FAILURE at target's channel → try smaller amount via probe
        total_hops = len(route_msg.hops)
        if failure and code_num == 15 and fsi is not None and fsi == total_hops - 2:
            probed = await probe_route_amount(
                routerstub, hop_keys, source_chan_id, cltv_delta, rebalance.value,
            )
            if probed > 0:
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Probe found max {probed} sats via {src_label}")
                probe_invoice = stub.AddInvoice(ln.Invoice(value=probed, expiry=timeout))
                try:
                    probe_build = await routerstub.BuildRoute(
                        lnr.BuildRouteRequest(
                            outgoing_chan_id=int(source_chan_id),
                            amt_msat=probed * 1000,
                            hop_pubkeys=hop_keys,
                            final_cltv_delta=cltv_delta,
                            payment_addr=probe_invoice.payment_addr,
                        )
                    )
                except Exception:
                    continue
                # Opp-cost check for probed amount
                p_allowed, p_ppm, p_max = check_opportunity_cost(
                    probe_build.route.total_fees_msat, probed,
                    source_chan_id, source_fee_map,
                    target_fee_rate, ar_max_cost, max_fee_rate,
                )
                if not p_allowed:
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Probed route via {src_label} rejected: {p_ppm} ppm > {p_max} ppm")
                    continue
                try:
                    probe_response = await routerstub.SendToRouteV2(
                        lnr.SendToRouteRequest(
                            payment_hash=probe_invoice.r_hash,
                            route=probe_build.route,
                        ),
                        timeout=timeout,
                    )
                except Exception:
                    continue
                if probe_response.status == 1:
                    fees_msat = probe_response.route.total_fees_msat or 0
                    probe_hex = probe_build.route.SerializeToString().hex()
                    await update_route(rebalance.last_hop_pubkey, source_chan_id, probe_hex, True, forgive_failure=True)
                    await update_node_reputations(probe_hex, True)
                    # Scale fee_limit so RapidFire children keep same ppm
                    scaled_fee_limit = round(rebalance.fee_limit * (probed / rebalance.value), 3)
                    return {
                        'status': 2,
                        'fees_paid': fees_msat / 1000 if fees_msat else 0,
                        'successful_out': probe_response.route.hops[0].chan_id,
                        'successful_in': probe_response.route.hops[-1].chan_id,
                        'payment_hash': probe_invoice.r_hash.hex(),
                        'value': probed,
                        'scaled_fee_limit': scaled_fee_limit,
                    }
        # Route failed, caller should try next source
        return None

    return None


@sync_to_async
def get_out_cans(rebalance, auto_rebalance_channels):
    try:
        exclude_keys = {rebalance.last_hop_pubkey}
        result = list(
            auto_rebalance_channels
            .filter(percent_outbound__gte=F('ar_out_target'))
            .filter(
                Q(ar_source=True)        # explicitly enabled as source in Advanced Rebalancing
                | Q(auto_rebalance=False) # non-AR-managed channels are free to drain
            )
            .exclude(remote_pubkey__in=exclude_keys)
            .order_by('htlc_count')
            .values_list('chan_id', flat=True)
        )
        if len(result) > 1:
            alias_lookup = dict(Channels.objects.filter(chan_id__in=[str(c) for c in result]).values_list('chan_id', 'alias'))
            labeled = [f"{c} ({alias_lookup.get(str(c)) or '?'})" for c in result]
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : get_out_cans: Found {len(result)} candidate channels (ordered by DB htlc_count): {labeled}")
        return result
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting outbound cands: {str(e)}")

@sync_to_async
def save_record(record):
    try:
        record.save()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error saving database record: {str(e)}")

@sync_to_async
def inbound_cans_len(inbound_cans):
    try:
        return len(inbound_cans)
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting inbound cands: {str(e)}")

@sync_to_async
def get_max_fee_rate():
    return get_local_setting('AR-MaxFeeRate', 500, int)

@sync_to_async
def get_per_source_enabled():
    """Feature flag: when 1, use per-source iteration instead of SendPaymentV2 fallback."""
    return get_local_setting('AR-PerSourceEnabled', 0, int) == 1

@sync_to_async
def get_allowed_sources_for_target(target_pubkey):
    """Return set of chan_ids if AllowedTargets are configured for this target, else None."""
    entries = AllowedTarget.objects.filter(target_pubkey=target_pubkey).select_related('source_chan')
    ids = {e.source_chan.chan_id for e in entries}
    return ids if ids else None

@sync_to_async
def get_source_fee_map(chan_ids):
    """Build a map of chan_id -> local_fee_rate for outbound channels."""
    return dict(
        Channels.objects.filter(chan_id__in=[str(c) for c in chan_ids])
        .values_list('chan_id', 'local_fee_rate')
    )

@sync_to_async
def get_source_diff_map(chan_ids):
    """Build a map of chan_id -> ar_source_ppm_diff for outbound channels."""
    return dict(
        Channels.objects.filter(chan_id__in=[str(c) for c in chan_ids])
        .values_list('chan_id', 'ar_source_ppm_diff')
    )

@sync_to_async
def get_source_alias_map(chan_ids):
    """Build a map of chan_id -> alias for log readability."""
    return dict(
        Channels.objects.filter(chan_id__in=[str(c) for c in chan_ids])
        .values_list('chan_id', 'alias')
    )

def _label(cid, alias_map):
    """Format a chan_id as 'chan_id (alias)' for logs; falls back to plain chan_id."""
    alias = alias_map.get(str(cid)) or alias_map.get(cid)
    return f"{cid} ({alias})" if alias else str(cid)

@sync_to_async
def get_target_info(last_hop_pubkey):
    """Return (local_fee_rate, ar_max_cost) for the target channel, or None."""
    ch = Channels.objects.filter(
        is_open=True, auto_rebalance=True, remote_pubkey=last_hop_pubkey
    ).first()
    if ch:
        return ch.local_fee_rate, ch.ar_max_cost
    return None, None

def check_opportunity_cost(route_fee_msat, amount_sat, source_chan_id, source_fee_map, target_fee_rate, ar_max_cost, max_fee_rate):
    """Check if route fee fits within budget after deducting source opportunity cost.

    Returns (allowed, route_fee_ppm, max_route_ppm) tuple.
    Formula: max_route_fee = (target_fee * ar_max_cost%) - source_outbound_fee
    The source's outbound fee is an opportunity cost (lost forwarding revenue)
    that must be subtracted from the rebalancing budget."""
    source_fee = source_fee_map.get(str(source_chan_id), source_fee_map.get(source_chan_id, 0))
    max_route_ppm = int(target_fee_rate * (ar_max_cost / 100)) - source_fee
    max_route_ppm = min(max_route_ppm, max_fee_rate)
    route_fee_ppm = int((route_fee_msat / (amount_sat * 1000)) * 1000000) if amount_sat > 0 else 0
    return route_fee_ppm <= max_route_ppm, route_fee_ppm, max_route_ppm

def get_active_rebalance_pubkeys():
    try:
        return set(
            Rebalancer.objects
            .filter(status__in=[0, 1])
            .exclude(last_hop_pubkey="")
            .values_list("last_hop_pubkey", flat=True)
        )
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting active pubkeys: {str(e)}")
        return set()

@sync_to_async
def get_route_limit():
    try:
        return get_local_setting('RR-RouteLimit', 10, int)
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting route limit: {str(e)}")
        return 10

@sync_to_async
def routes_collection_enabled():
    try:
        return get_local_setting('RR-CollectRoutes', '1', str) != '0'
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting route collect setting: {str(e)}")
        return True

@sync_to_async
def saved_routes_enabled():
    try:
        return get_local_setting('RR-UseSavedRoutes', '1', str) != '0'
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting saved route setting: {str(e)}")
        return True

@sync_to_async
def get_saved_routes(pubkey, chan_ids, limit=10):
    try:
        cutoff = timezone.now() - timedelta(minutes=30)
        # Fetch tested and untested candidates separately so untested can be randomized
        fetch_limit = max(limit * 3, 30)
        base_qs = (
            RebalanceRoute.objects
            .filter(target_pubkey=pubkey, outgoing_chan_id__in=chan_ids)
            .filter(Q(last_failure__lt=cutoff) | Q(last_failure__isnull=True))
            .annotate(
                ratio=calc_success_ratio(F('success_count'), F('failure_count')),
                weighted_ratio=calc_weighted_ratio(F('success_count'), F('failure_count')),
                is_untested=Case(
                    When(success_count=0, failure_count=0, then=Value(1)),
                    default=Value(0),
                    output_field=IntegerField(),
                ),
            )
        )
        # Fetch truly untested (no fee data) randomized, then fee-known untested, then tested
        truly_untested = list(
            base_qs.filter(success_count=0, failure_count=0, last_fee_ppm__isnull=True)
            .order_by('?')[:fetch_limit]
        )
        fee_known_untested = list(
            base_qs.filter(success_count=0, failure_count=0, last_fee_ppm__isnull=False)
            .order_by('last_fee_ppm')[:fetch_limit]
        )
        tested = list(
            base_qs.exclude(success_count=0, failure_count=0)
            .order_by('-weighted_ratio')[:fetch_limit]
        )
        candidates = truly_untested + fee_known_untested + tested
        candidates = candidates[:fetch_limit]
        if not candidates:
            return []

        # Step 1: Node reputation scoring
        route_hops = {}
        all_pubkeys = set()
        for r in candidates:
            hops = set(r.route.split('-'))
            route_hops[r.id] = hops
            all_pubkeys.update(hops)

        rep_map = {}
        if all_pubkeys:
            for nr in NodeReputation.objects.filter(pubkey__in=all_pubkeys):
                rep_map[nr.pubkey] = calc_weighted_ratio(nr.success_count, nr.failure_count)

        # Compute min fee among candidates for relative fee scoring
        now = timezone.now()
        fee_values = [r.last_fee_ppm for r in candidates if r.last_fee_ppm and r.last_fee_ppm > 0]
        min_fee = min(fee_values) if fee_values else None

        scored = []
        for r in candidates:
            hops = route_hops[r.id]
            if hops and rep_map:
                node_scores = [rep_map.get(pk, 0.5) for pk in hops]
                min_node_score = min(node_scores)
            else:
                min_node_score = 0.5

            # Untested probed routes: score higher than low-quality tested routes
            # If we already know the fee from a previous BuildRoute, deprioritize expensive ones
            is_untested = r.success_count == 0 and r.failure_count == 0
            if is_untested:
                if min_fee and r.last_fee_ppm and r.last_fee_ppm > 0:
                    fee_factor = min_fee / r.last_fee_ppm
                else:
                    fee_factor = 1.0
                adj_score = 0.5 * min_node_score * fee_factor
            else:
                # Time-decay: half-life of 48 hours
                if r.last_success:
                    hours_ago = (now - r.last_success).total_seconds() / 3600
                    time_factor = 2 ** (-hours_ago / 48)
                else:
                    time_factor = 0.1

                # Fee scoring: cheapest route gets 1.0, others proportionally less
                if min_fee and r.last_fee_ppm and r.last_fee_ppm > 0:
                    fee_factor = min_fee / r.last_fee_ppm
                else:
                    fee_factor = 0.5

                adj_score = r.weighted_ratio * min_node_score * time_factor * fee_factor
            scored.append((r, adj_score, hops))

        # Step 2: Diversity-aware greedy selection
        scored.sort(key=lambda x: x[1], reverse=True)
        selected = []
        used_hops = set()
        remaining = list(scored)

        while remaining and len(selected) < limit:
            best_idx = 0
            best_final = -1
            for i, (r, adj_score, hops) in enumerate(remaining):
                shared = len(hops & used_hops)
                penalty = 0.5 ** shared
                final = adj_score * penalty
                if final > best_final:
                    best_final = final
                    best_idx = i
            r, adj_score, hops = remaining.pop(best_idx)
            selected.append(r)
            used_hops.update(hops)

        # Step 3: Ensure exploration — reserve ~50% of slots for truly untested routes
        # (no fee data at all, never even had a BuildRoute succeed)
        min_explore = max(1, limit // 2)
        untested_count = sum(1 for r in selected if r.success_count == 0 and r.failure_count == 0 and not r.last_fee_ppm)
        if untested_count < min_explore:
            selected_ids = {r.id for r in selected}
            untested_avail = [
                r for r, s, h in scored
                if r.success_count == 0 and r.failure_count == 0 and not r.last_fee_ppm and r.id not in selected_ids
            ]
            need = min(min_explore - untested_count, len(untested_avail))
            if need > 0:
                # Drop lowest-priority tested routes (last picked by diversity)
                keep = []
                can_drop = need
                for r in reversed(selected):
                    if can_drop > 0 and (r.success_count > 0 or r.failure_count > 0):
                        can_drop -= 1
                    else:
                        keep.append(r)
                keep.reverse()
                keep.extend(untested_avail[:need])
                selected = keep

        return selected
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting saved routes: {str(e)}")
        return []

@sync_to_async
def update_route(pubkey, chan_id, route_hex, success=True, forgive_failure=False):
    try:
        if get_local_setting('RR-CollectRoutes', '1', str) == '0':
            return
        parsed = ln.Route()
        parsed.ParseFromString(bytes.fromhex(route_hex))
        path = "-".join(h.pub_key for h in parsed.hops)
        if len(parsed.hops) >= 2:
            cltv = parsed.hops[-1].expiry - parsed.hops[-2].expiry
        else:
            cltv = 144
        route_obj, _ = RebalanceRoute.objects.get_or_create(
            target_pubkey=pubkey,
            outgoing_chan_id=chan_id,
            route=path,
            defaults={"final_cltv_delta": cltv, "route_hex": route_hex},
        )
        if route_obj.final_cltv_delta != cltv:
            route_obj.final_cltv_delta = cltv
        if not route_obj.route_hex:
            route_obj.route_hex = route_hex
        now = timezone.now()
        if success:
            if not route_obj.last_success or now - route_obj.last_success > timedelta(minutes=5):
                route_obj.success_count += 1
            route_obj.last_success = now
            route_obj.last_failure = None
            if forgive_failure and route_obj.failure_count > 0:
                route_obj.failure_count -= 1
            if parsed.total_amt_msat and parsed.total_fees_msat:
                route_obj.last_fee_ppm = (parsed.total_fees_msat / (parsed.total_amt_msat - parsed.total_fees_msat)) * 1000000
        else:
            route_obj.failure_count += 1
            route_obj.last_failure = now
        route_obj.save()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error updating route record: {str(e)}")

@sync_to_async
def update_route_fee(sr, fee_ppm):
    """Store last_fee_ppm on a route without counting success or failure."""
    try:
        sr.last_fee_ppm = fee_ppm
        sr.save(update_fields=['last_fee_ppm'])
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error updating route fee: {str(e)}")

@sync_to_async
def update_node_reputations(route_hex, success, failure_source_index=None):
    try:
        if get_local_setting('RR-CollectRoutes', '1', str) == '0':
            return
        parsed = ln.Route()
        parsed.ParseFromString(bytes.fromhex(route_hex))
        hops = list(parsed.hops)
        if not hops:
            return
        now = timezone.now()
        if success:
            for hop in hops:
                node, _ = NodeReputation.objects.get_or_create(pubkey=hop.pub_key)
                node.success_count += 1
                node.last_success = now
                node.save()
        elif failure_source_index is not None and failure_source_index < len(hops):
            for i, hop in enumerate(hops):
                if i < failure_source_index:
                    node, _ = NodeReputation.objects.get_or_create(pubkey=hop.pub_key)
                    node.success_count += 1
                    node.last_success = now
                    node.save()
                elif i == failure_source_index:
                    node, _ = NodeReputation.objects.get_or_create(pubkey=hop.pub_key)
                    node.failure_count += 1
                    node.last_failure = now
                    node.save()
                    break
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error updating node reputations: {str(e)}")

@sync_to_async
def purge_stale_routes():
    try:
        if get_local_setting('RR-CollectRoutes', '1', str) == '0':
            return
        # Delete routes whose outgoing channel is no longer open
        open_chan_ids = set(Channels.objects.filter(is_open=True).values_list('chan_id', flat=True))
        dead_routes = RebalanceRoute.objects.exclude(outgoing_chan_id__in=open_chan_ids).delete()
        if dead_routes[0]:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Purged {dead_routes[0]} routes for closed outgoing channels")
        cutoff = timezone.now() - timedelta(days=7)
        # Delete tested routes with no recent success, but keep untested
        # probed routes so they get a chance to be tried first
        RebalanceRoute.objects.filter(
            Q(last_success__lt=cutoff) | Q(last_success__isnull=True)
        ).exclude(
            success_count=0, failure_count=0
        ).delete()
        rep_cutoff = timezone.now() - timedelta(days=14)
        NodeReputation.objects.filter(
            Q(last_success__lt=rep_cutoff) | Q(last_success__isnull=True),
            Q(last_failure__lt=rep_cutoff) | Q(last_failure__isnull=True),
        ).delete()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error purging stale routes: {str(e)}")

@sync_to_async
def mark_route_failure(sr):
    try:
        if get_local_setting('RR-CollectRoutes', '1', str) == '0':
            return
        sr.failure_count += 1
        sr.last_failure = timezone.now()
        sr.save(update_fields=['failure_count', 'last_failure'])
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error marking route failure: {str(e)}")

@sync_to_async
def check_and_set_allow_multishards():
    allow_multishards = True
    disable_val = get_local_setting('LND-DisableMPP', 0, int)
    if disable_val > 0:
        allow_multishards = False
    return allow_multishards

def sort_channels_by_htlc(stub, chan_ids):
    """Filter channels: for peers with multiple channels, only keep the one with lowest HTLC count"""
    try:
        if len(chan_ids) <= 1:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Only 1 channel, no filtering needed")
            return chan_ids

        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Checking {len(chan_ids)} channels from DB")

        # Pre-fetch aliases for log readability
        alias_map = dict(Channels.objects.filter(chan_id__in=[str(c) for c in chan_ids]).values_list('chan_id', 'alias'))

        # Get current channel state from LND
        channels = stub.ListChannels(ln.ListChannelsRequest(active_only=False)).channels

        # Build maps for channels we care about
        chan_data = {}  # chan_id -> {htlc_count, remote_pubkey}
        for c in channels:
            if str(c.chan_id) in [str(x) for x in chan_ids]:
                htlc_count = len(c.pending_htlcs)
                chan_data[str(c.chan_id)] = {
                    'htlc_count': htlc_count,
                    'remote_pubkey': c.remote_pubkey
                }
                alias = alias_map.get(str(c.chan_id)) or "?"
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Chan {c.chan_id} ({alias}) has {htlc_count} pending HTLCs")

        # Group channels by peer
        peer_channels = {}  # remote_pubkey -> [(chan_id, htlc_count), ...]
        for chan_id in chan_ids:
            chan_id_str = str(chan_id)
            if chan_id_str in chan_data:
                peer = chan_data[chan_id_str]['remote_pubkey']
                htlc_count = chan_data[chan_id_str]['htlc_count']
                if peer not in peer_channels:
                    peer_channels[peer] = []
                peer_channels[peer].append((chan_id, htlc_count))

        # For each peer, select only the channel with lowest HTLC count
        filtered_ids = []
        for peer, peer_chan_list in peer_channels.items():
            if len(peer_chan_list) > 1:
                # Multiple channels to same peer - pick the one with lowest HTLC count
                sorted_peer_chans = sorted(peer_chan_list, key=lambda x: x[1])
                selected = sorted_peer_chans[0]
                filtered_ids.append(selected[0])
                peer_alias = alias_map.get(str(selected[0])) or "?"
                sel_label = f"{selected[0]} ({peer_alias})"
                excl_labels = [f"{c[0]} ({alias_map.get(str(c[0])) or '?'}; {c[1]} HTLCs)" for c in sorted_peer_chans[1:]]
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Peer {peer_alias} has {len(peer_chan_list)} channels - selected {sel_label} with {selected[1]} HTLCs, excluded: {excl_labels}")
            else:
                # Single channel to this peer - include it
                filtered_ids.append(peer_chan_list[0][0])

        orig_labels = [f"{c} ({alias_map.get(str(c)) or '?'})" for c in chan_ids]
        filt_labels = [f"{c} ({alias_map.get(str(c)) or '?'})" for c in filtered_ids]
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Original list: {orig_labels}")
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Filtered list: {filt_labels} (removed {len(chan_ids) - len(filtered_ids)} channels)")

        return filtered_ids
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error filtering channels by HTLC: {str(e)}")
        return chan_ids

async def run_rebalancer(rebalance, worker):
    try:
        # Check if LocalSetting LND-EnableMPP exists and set allow_mpp accordingly
        allow_multishards = await check_and_set_allow_multishards()  # Default value is True.
        max_parts = None if allow_multishards else 1  # Adjust max_parts based on the allow_multishards value
        #Reduce potential rebalance value in percent out to avoid going below AR-OUT-Target,
        # accounting for local_chan_reserve so we don't count sats we can't actually spend.
        auto_rebalance_channels = Channels.objects.filter(is_active=True, is_open=True, private=False).annotate(percent_outbound=((Sum('local_balance')+Sum('pending_outbound')-Sum('local_chan_reserve')-rebalance.value)*100)/Sum('capacity')).annotate(inbound_can=(((Sum('remote_balance')+Sum('pending_inbound'))*100)/Sum('capacity'))/Sum('ar_in_target'))
        outbound_cans = await get_out_cans(rebalance, auto_rebalance_channels)
        if len(outbound_cans) == 0 and rebalance.manual == False:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : No outbound_cans")
            rebalance.status = 406
            rebalance.start = datetime.now()
            rebalance.stop = datetime.now()
            await save_record(rebalance)
            return None
        elif str(outbound_cans).replace('\'', '') != rebalance.outgoing_chan_ids and rebalance.manual == False:
            rebalance.outgoing_chan_ids = str(outbound_cans).replace('\'', '')
        rebalance.start = datetime.now()
        successful_out = None
        successful_in = None
        try:
            # Use shared channels for both sync and async stubs
            channel = get_shared_channel()
            async_channel = get_shared_async_channel()
            stub = lnrpc.LightningStub(channel)
            routerstub = lnrouter.RouterStub(async_channel)
            chan_ids = json.loads(rebalance.outgoing_chan_ids)
            # Filter channels: for peers with multiple channels, only send the one with lowest HTLC count
            chan_ids = sort_channels_by_htlc(stub, chan_ids)

            # Load opportunity cost data for this rebalance
            source_fee_map = await get_source_fee_map(chan_ids)
            source_diff_map = await get_source_diff_map(chan_ids)
            source_alias_map = await get_source_alias_map(chan_ids)
            target_fee_rate, ar_max_cost = await get_target_info(rebalance.last_hop_pubkey)
            max_fee_rate = await get_max_fee_rate()
            opp_cost_enabled = target_fee_rate is not None and ar_max_cost is not None

            # Pre-filter outbound channels: drop sources whose own ar_source_ppm_diff
            # spread requirement isn't met by this target, and those whose opportunity
            # cost alone exceeds the budget (target_fee * ar_max_cost%)
            if opp_cost_enabled and not rebalance.manual:
                original_count = len(chan_ids)
                filtered_ids = []
                for cid in chan_ids:
                    src_fee = source_fee_map.get(str(cid), source_fee_map.get(cid, 0))
                    src_diff = source_diff_map.get(str(cid), source_diff_map.get(cid, 0)) or 0
                    if target_fee_rate < src_fee + src_diff:
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Excluding source {_label(cid, source_alias_map)} (target fee {target_fee_rate} < source {src_fee} + ppm_diff {src_diff} for {rebalance.target_alias})")
                        continue
                    max_route_ppm = int(target_fee_rate * (ar_max_cost / 100)) - src_fee
                    if max_route_ppm > 0:
                        filtered_ids.append(cid)
                    else:
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Excluding source {_label(cid, source_alias_map)} (outbound fee {src_fee} ppm eats entire budget for target {rebalance.target_alias})")
                chan_ids = filtered_ids
                if len(chan_ids) < original_count:
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Opportunity cost filter: {original_count} -> {len(chan_ids)} outbound channels")
                if len(chan_ids) == 0:
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : No outbound channels left after opportunity cost filter")
                    rebalance.status = 406
                    rebalance.start = datetime.now()
                    rebalance.stop = datetime.now()
                    await save_record(rebalance)
                    return None

            timeout = rebalance.duration * 60
            invoice_response = stub.AddInvoice(
                ln.Invoice(value=rebalance.value, expiry=timeout)
            )
            # record the payment hash early so logs show it even when using
            # SendToRouteV2 which doesn't stream hash updates
            rebalance.payment_hash = invoice_response.r_hash.hex()
            print(
                f"{datetime.now().strftime('%c')} : [Rebalancer] : {worker} starting rebalance for {rebalance.target_alias} {rebalance.last_hop_pubkey} for {rebalance.value} sats and duration {rebalance.duration}, using {len(chan_ids)} outbound channels"
            )

            use_saved = await saved_routes_enabled()
            if use_saved:
                await purge_stale_routes()
                route_limit = await get_route_limit()
                saved_routes = await get_saved_routes(rebalance.last_hop_pubkey, chan_ids, limit=route_limit)
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Loaded {len(saved_routes)} saved routes to try")
            else:
                saved_routes = []
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved routes disabled")
            fee_limit_msat = int(rebalance.fee_limit * 1000)
            payment_response = None
            tried_saved_sources = set()  # chan_ids actually attempted on the wire via saved routes
            saved_alias_map = await get_source_alias_map([sr.outgoing_chan_id for sr in saved_routes]) if saved_routes else {}
            for sr in saved_routes:
                sr_label = _label(sr.outgoing_chan_id, saved_alias_map)
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Trying saved route via {sr_label}")
                rebuilt_hex = None
                try:
                    if sr.route_hex:
                        parsed_sr = ln.Route()
                        parsed_sr.ParseFromString(bytes.fromhex(sr.route_hex))
                        hop_keys = [bytes.fromhex(h.pub_key) for h in parsed_sr.hops]
                        if sr.final_cltv_delta:
                            cltv_delta = sr.final_cltv_delta
                        elif len(parsed_sr.hops) >= 2:
                            cltv_delta = parsed_sr.hops[-1].expiry - parsed_sr.hops[-2].expiry
                        else:
                            cltv_delta = 144
                    else:
                        hop_keys = [
                            bytes.fromhex(k.decode() if isinstance(k, (bytes, bytearray)) else k)
                            for k in sr.route.split('-')
                        ]
                        cltv_delta = sr.final_cltv_delta or 144
                    # Cap by smallest max_htlc_msat across the route's hops to skip
                    # routes that physically can't carry this amount.
                    if sr.route_hex:
                        cap_msat = await sync_to_async(_max_amount_on_route_msat)(stub, parsed_sr)
                        if cap_msat and rebalance.value * 1000 > cap_msat:
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved route via {sr_label} max_htlc cap is {cap_msat // 1000} sats, less than rebalance value {rebalance.value} - skipping"
                            )
                            continue
                        # Skip routes that reuse a known-bad edge at a similar amount.
                        if not _validate_route(parsed_sr):
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved route via {sr_label} skipped — reuses a cached failed edge"
                            )
                            continue
                    build = await routerstub.BuildRoute(
                        lnr.BuildRouteRequest(
                            outgoing_chan_id=int(sr.outgoing_chan_id),
                            amt_msat=rebalance.value * 1000,
                            hop_pubkeys=hop_keys,
                            final_cltv_delta=cltv_delta,
                            payment_addr=invoice_response.payment_addr,
                        )
                    )
                    print(
                        f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute succeeded via {sr_label}"
                    )

                    # Check opportunity cost: route_fee + source_outbound_fee must fit within per-source budget
                    if opp_cost_enabled and not rebalance.manual:
                        allowed, total_ppm, budget_ppm = check_opportunity_cost(
                            build.route.total_fees_msat, rebalance.value,
                            sr.outgoing_chan_id, source_fee_map,
                            target_fee_rate, ar_max_cost, max_fee_rate
                        )
                        if not allowed:
                            src_fee = source_fee_map.get(str(sr.outgoing_chan_id), source_fee_map.get(sr.outgoing_chan_id, 0))
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute via {sr_label} rejected: route {total_ppm} ppm + source {src_fee} ppm outbound, max route budget {budget_ppm} ppm (target {target_fee_rate} * {ar_max_cost}% - {src_fee} source) - skipping"
                            )
                            payment_response = None
                            continue

                    if build.route.total_fees_msat > fee_limit_msat:
                        actual_ppm = (build.route.total_fees_msat / (rebalance.value * 1000)) * 1000000
                        print(
                            f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute via {sr_label} exceeds fee limit ({build.route.total_fees_msat} > {fee_limit_msat} msat, {int(actual_ppm)} ppm) - skipping without penalty"
                        )
                        await update_route_fee(sr, actual_ppm)
                        payment_response = None
                        continue

                    route_msg = build.route
                    rebuilt_hex = route_msg.SerializeToString().hex()
                    # Only mark the source as tried once we actually hit the wire,
                    # so per-source iteration is free to try sources we never
                    # actually attempted via saved routes (e.g. opp-cost rejected).
                    tried_saved_sources.add(str(sr.outgoing_chan_id))
                    payment_response = await routerstub.SendToRouteV2(
                        lnr.SendToRouteRequest(
                            payment_hash=invoice_response.r_hash,
                            route=route_msg,
                        ),
                        timeout=timeout,
                    )
                    # HTLCStatus uses 1 for SUCCEEDED
                    if payment_response.status == 1:
                        rebalance.status = 2
                        fees_msat = getattr(payment_response, "fee_msat", None)
                        if not fees_msat:
                            try:
                                fees_msat = payment_response.route.total_fees_msat
                            except Exception:
                                fees_msat = 0
                        rebalance.fees_paid = fees_msat / 1000 if fees_msat else 0
                        try:
                            successful_out = payment_response.route.hops[0].chan_id
                            successful_in = payment_response.route.hops[-1].chan_id
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved route succeeded via {sr_label} - hash: {rebalance.payment_hash}"
                            )
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : Used outgoing chan_id: {successful_out}, incoming chan_id: {successful_in}"
                            )
                        except Exception:
                            successful_out = None
                            successful_in = None
                        await update_route(
                            rebalance.last_hop_pubkey, sr.outgoing_chan_id, rebuilt_hex, True
                        )
                        await update_node_reputations(rebuilt_hex, True)
                        break
                    else:
                        failure = getattr(payment_response, "failure", None)
                        fsi = None
                        if failure:
                            code_num = getattr(failure, "code", None)
                            reason = FAILURE_CODE_NAMES.get(code_num, code_num)
                            detail_num = getattr(failure, "failure_detail", None)
                            if detail_num is None:
                                detail = "not set"
                            else:
                                detail = FAILURE_DETAIL_NAMES.get(detail_num, detail_num)
                            fsi = getattr(failure, "failure_source_index", None)
                        else:
                            reason = "no-details"
                            detail = "not set"
                        print(
                            f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved route failed via {sr_label} - code: {reason} - detail: {detail} - failure_hop: {fsi}"
                        )
                        _record_route_failure(route_msg, failure)
                        await update_route(
                            rebalance.last_hop_pubkey, sr.outgoing_chan_id, rebuilt_hex, False
                        )
                        await update_node_reputations(rebuilt_hex, False, fsi)
                        # Probe if target peer lacks liquidity
                        total_hops = len(route_msg.hops)
                        if failure and code_num == 15 and fsi is not None and fsi == total_hops - 2:
                            probed = await probe_route_amount(
                                routerstub, hop_keys, sr.outgoing_chan_id,
                                cltv_delta, rebalance.value,
                            )
                            if probed > 0:
                                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Probe found max {probed} sats (was {rebalance.value})")
                                probe_invoice = stub.AddInvoice(ln.Invoice(value=probed, expiry=timeout))
                                probe_build = await routerstub.BuildRoute(
                                    lnr.BuildRouteRequest(
                                        outgoing_chan_id=int(sr.outgoing_chan_id),
                                        amt_msat=probed * 1000,
                                        hop_pubkeys=hop_keys,
                                        final_cltv_delta=cltv_delta,
                                        payment_addr=probe_invoice.payment_addr,
                                    )
                                )
                                scaled_fee_limit = fee_limit_msat * probed // rebalance.value
                                # Also check opportunity cost for probed amount
                                opp_ok = True
                                if opp_cost_enabled and not rebalance.manual:
                                    opp_ok, opp_total, opp_budget = check_opportunity_cost(
                                        probe_build.route.total_fees_msat, probed,
                                        sr.outgoing_chan_id, source_fee_map,
                                        target_fee_rate, ar_max_cost, max_fee_rate
                                    )
                                    if not opp_ok:
                                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Probe via {sr_label} rejected: route {opp_total} ppm exceeds budget {opp_budget} ppm after opportunity cost")
                                if opp_ok and probe_build.route.total_fees_msat <= scaled_fee_limit:
                                    probe_response = await routerstub.SendToRouteV2(
                                        lnr.SendToRouteRequest(
                                            payment_hash=probe_invoice.r_hash,
                                            route=probe_build.route,
                                        ),
                                        timeout=timeout,
                                    )
                                    if probe_response.status == 1:  # SUCCEEDED
                                        # Scale fee_limit proportionally so RapidFire children keep the same ppm
                                        rebalance.fee_limit = round(rebalance.fee_limit * (probed / rebalance.value), 3)
                                        rebalance.value = probed
                                        rebalance.status = 2
                                        rebalance.payment_hash = probe_invoice.r_hash.hex()
                                        fees_msat = probe_response.route.total_fees_msat or 0
                                        rebalance.fees_paid = fees_msat / 1000
                                        successful_out = probe_response.route.hops[0].chan_id
                                        successful_in = probe_response.route.hops[-1].chan_id
                                        rebuilt_hex = probe_build.route.SerializeToString().hex()
                                        await update_route(rebalance.last_hop_pubkey, sr.outgoing_chan_id, rebuilt_hex, True, forgive_failure=True)
                                        await update_node_reputations(rebuilt_hex, True)
                                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Probe payment succeeded: {probed} sats via {sr_label}")
                                        break
                except Exception as e:
                    print(
                        f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute failed via {sr_label} - {e}"
                    )
                    if rebuilt_hex is not None:
                        await update_route(
                            rebalance.last_hop_pubkey,
                            sr.outgoing_chan_id,
                            rebuilt_hex,
                            False,
                        )
                    else:
                        await mark_route_failure(sr)
                    payment_response = None

            per_source_enabled = opp_cost_enabled and not rebalance.manual and await get_per_source_enabled()

            if per_source_enabled and (payment_response is None or payment_response.status != 1):
                # Per-source iteration (regolancer-style): each source gets its own fee_limit accounting
                # for opportunity cost. No SendPaymentV2 fallback when this path runs.
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Per-source iteration enabled, target budget {int(target_fee_rate * ar_max_cost / 100)} ppm")
                allowed_sources = await get_allowed_sources_for_target(rebalance.last_hop_pubkey)

                # Sort sources by outbound fee ascending (cheapest first)
                ordered_sources = sorted(
                    chan_ids,
                    key=lambda c: source_fee_map.get(str(c), source_fee_map.get(c, 0))
                )
                # Skip sources already tried as saved routes
                ordered_sources = [c for c in ordered_sources if str(c) not in tried_saved_sources]
                # Restrict to allowed targets if configured
                if allowed_sources is not None:
                    ordered_sources = [c for c in ordered_sources if str(c) in {str(x) for x in allowed_sources}]

                for source_chan in ordered_sources:
                    # Reuse the invoice created earlier (failed SendToRouteV2 attempts
                    # don't settle the invoice, same pattern as saved routes). Workers
                    # can use the same source concurrently — Lightning handles multiple
                    # in-flight HTLCs per channel; AR-DispatchInterval staggers naturally.
                    result = await try_single_source(
                        stub, routerstub, rebalance, source_chan, source_fee_map,
                        target_fee_rate, ar_max_cost, max_fee_rate,
                        invoice_response, fee_limit_msat, timeout,
                    )

                    if result is not None:
                        # Success via this source
                        rebalance.status = 2
                        rebalance.fees_paid = result['fees_paid']
                        rebalance.payment_hash = result['payment_hash']
                        if 'scaled_fee_limit' in result:
                            rebalance.fee_limit = result['scaled_fee_limit']
                        if result['value'] != rebalance.value:
                            rebalance.value = result['value']
                        successful_out = result['successful_out']
                        successful_in = result['successful_in']
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Per-source succeeded via {_label(source_chan, source_alias_map)} - hash: {rebalance.payment_hash}")
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Used outgoing chan_id: {successful_out}, incoming chan_id: {successful_in}")
                        payment_response = None  # signal success to RapidFire via rebalance.status=2
                        break
                    # Edge-level failure caching now happens inside try_single_source
                    # right after each SendToRouteV2 attempt, so we don't blanket-skip
                    # the whole (source, target) pair here anymore.

                if rebalance.status != 2:
                    # No source succeeded
                    if rebalance.status == 0:
                        rebalance.status = 4  # FAILURE_REASON_NO_ROUTE
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Per-source iteration: no route succeeded for {rebalance.target_alias}")
            elif payment_response is None or payment_response.status != 1:
                    if saved_routes:
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Falling back to automatic routing")
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : SendPaymentV2: Sending to LND with outgoing_chan_ids={chan_ids}")
                    async for payment_response in routerstub.SendPaymentV2(lnr.SendPaymentRequest(payment_request=str(invoice_response.payment_request), fee_limit_msat=int(rebalance.fee_limit*1000), outgoing_chan_ids=chan_ids, last_hop_pubkey=bytes.fromhex(rebalance.last_hop_pubkey), timeout_seconds=(timeout-5), allow_self_payment=True, max_parts=max_parts), timeout=(timeout+60)):
                        if payment_response.status == 1 and rebalance.status == 0:
                            #IN-FLIGHT
                            rebalance.payment_hash = payment_response.payment_hash
                            rebalance.status = 1
                            await save_record(rebalance)
                        elif payment_response.status == 2:
                            #SUCCESSFUL
                            rebalance.status = 2
                            fees_msat = getattr(payment_response, "fee_msat", None)
                            if not fees_msat and payment_response.htlcs:
                                try:
                                    fees_msat = payment_response.htlcs[0].route.total_fees_msat
                                except Exception:
                                    fees_msat = 0
                            rebalance.fees_paid = fees_msat / 1000 if fees_msat else 0
                            successful_out = payment_response.htlcs[0].route.hops[0].chan_id
                            successful_in = payment_response.htlcs[0].route.hops[-1].chan_id
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : Automatic route (SendPaymentV2) succeeded - hash: {rebalance.payment_hash}"
                            )
                            print(
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : LND selected outgoing chan_id: {successful_out} from provided list: {chan_ids}"
                            )
                            route_hex = payment_response.htlcs[0].route.SerializeToString().hex()
                            out_chan = str(payment_response.htlcs[0].route.hops[0].chan_id)
                            await update_route(rebalance.last_hop_pubkey, out_chan, route_hex, True)
                            await update_node_reputations(route_hex, True)
                        elif payment_response.status == 3:
                            #FAILURE
                            if payment_response.htlcs:
                                route_hex = payment_response.htlcs[0].route.SerializeToString().hex()
                                out_chan = str(payment_response.htlcs[0].route.hops[0].chan_id)
                                await update_route(rebalance.last_hop_pubkey, out_chan, route_hex, False)
                                fsi = getattr(payment_response.htlcs[0].failure, "failure_source_index", None)
                                await update_node_reputations(route_hex, False, fsi)
                            if payment_response.failure_reason == 1:
                                #FAILURE_REASON_TIMEOUT
                                rebalance.status = 3
                            elif payment_response.failure_reason == 2:
                                #FAILURE_REASON_NO_ROUTE
                                rebalance.status = 4
                            elif payment_response.failure_reason == 3:
                                #FAILURE_REASON_ERROR
                                rebalance.status = 5
                            elif payment_response.failure_reason == 4:
                                #FAILURE_REASON_INCORRECT_PAYMENT_DETAILS
                                rebalance.status = 6
                            elif payment_response.failure_reason == 5:
                                #FAILURE_REASON_INSUFFICIENT_BALANCE
                                rebalance.status = 7
                        elif payment_response.status == 0:
                            rebalance.status = 400
        except Exception as e:
            if str(e.code()) == 'StatusCode.DEADLINE_EXCEEDED':
                rebalance.status = 408
            else:
                rebalance.status = 400
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error while sending payment: {str(e)}")
        finally:
            close_shared_channel()
            await close_shared_async_channel()
            rebalance.stop = datetime.now()
            await save_record(rebalance)
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : {worker} completed payment attempts for {rebalance.target_alias}: {rebalance.payment_hash}")
            original_alias = rebalance.target_alias
            inc=1.21
            dec=2
            if rebalance.status ==2:
                if successful_in is not None and successful_out is not None:
                    await update_channels(stub, successful_in, successful_out)
                #Reduce potential rebalance value in percent out to avoid going below AR-OUT-Target,
                # subtracting local_chan_reserve (sats we cant actually spend).
                auto_rebalance_channels = Channels.objects.filter(is_active=True, is_open=True, private=False).annotate(percent_outbound=((Sum('local_balance')+Sum('pending_outbound')-Sum('local_chan_reserve')-rebalance.value*inc)*100)/Sum('capacity')).annotate(inbound_can=(((Sum('remote_balance')+Sum('pending_inbound'))*100)/Sum('capacity'))/Sum('ar_in_target'))
                inbound_cans = auto_rebalance_channels.filter(remote_pubkey=rebalance.last_hop_pubkey).filter(auto_rebalance=True, inbound_can__gte=1)
                outbound_cans = await get_out_cans(rebalance, auto_rebalance_channels)
                if await inbound_cans_len(inbound_cans) > 0 and len(outbound_cans) > 0:
                    next_rebalance = Rebalancer(value=int(rebalance.value*inc), fee_limit=round(rebalance.fee_limit*inc, 3), outgoing_chan_ids=str(outbound_cans).replace('\'', ''), last_hop_pubkey=rebalance.last_hop_pubkey, target_alias=original_alias, duration=1, status=1)
                    await save_record(next_rebalance)
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : RapidFire increase for {next_rebalance.target_alias} from {rebalance.value} to {next_rebalance.value}")
                else:
                    next_rebalance = None
            # For failed rebalances, try in rapid fire with reduced balances until give up.
            elif rebalance.status > 2 and rebalance.value > 69420:
                #Previous Rapidfire with increased value failed, try with lower value up to 69420.
                if rebalance.duration > 1:
                    next_value = await estimate_liquidity ( payment_response )
                    if next_value < 1000:
                        next_rebalance = None
                        return next_rebalance
                else:
                    next_value = rebalance.value/dec

                inbound_cans = auto_rebalance_channels.filter(remote_pubkey=rebalance.last_hop_pubkey).filter(auto_rebalance=True, inbound_can__gte=1)
                if await inbound_cans_len(inbound_cans) > 0 and len(outbound_cans) > 0:
                    next_rebalance = Rebalancer(value=int(next_value), fee_limit=round(rebalance.fee_limit/(rebalance.value/next_value), 3), outgoing_chan_ids=str(outbound_cans).replace('\'', ''), last_hop_pubkey=rebalance.last_hop_pubkey, target_alias=original_alias, duration=1, status=1)
                    await save_record(next_rebalance)
                    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : RapidFire decrease for {next_rebalance.target_alias} from {rebalance.value} to {next_rebalance.value}")
                else:
                    next_rebalance = None
            else:
                next_rebalance = None
            return next_rebalance
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error running rebalance attempt: {str(e)}")

@sync_to_async
def estimate_liquidity( payment ):
    if payment is None:
        return 0
    try:
        estimated_liquidity = 0
        if payment.status == 3:
            attempt = None
            for attempt in payment.htlcs:
                total_hops=len(attempt.route.hops)
                if attempt.failure.failure_source_index == total_hops:
                    #Failure from last hop indicating liquidity available
                    estimated_liquidity = attempt.route.total_amt if attempt.route.total_amt > estimated_liquidity else estimated_liquidity
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Estimated Liquidity {estimated_liquidity} for payment {payment.payment_hash} with status {payment.status} and reason {payment.failure_reason}")
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error estimating liquidity: {str(e)}")
        estimated_liquidity = 0

    return estimated_liquidity

@sync_to_async
def update_channels(stub, incoming_chan_id, outgoing_chan_id):
    try:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : update_channels: Updating balances for incoming={incoming_chan_id}, outgoing={outgoing_chan_id}")
        # Incoming channel update
        channel = stub.ListChannels(ln.ListChannelsRequest(active_only=False)).channels
        incoming_channel = next((c for c in channel if c.chan_id == incoming_chan_id), None)
        if incoming_channel:
            db_channel = Channels.objects.filter(chan_id=incoming_chan_id).first()
            if db_channel:
                old_local = db_channel.local_balance
                old_remote = db_channel.remote_balance
                db_channel.local_balance = incoming_channel.local_balance
                db_channel.remote_balance = incoming_channel.remote_balance
                db_channel.save()
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : update_channels: Incoming chan {incoming_chan_id} - local: {old_local}->{incoming_channel.local_balance}, remote: {old_remote}->{incoming_channel.remote_balance}")
        # Outgoing channel update
        outgoing_channel = next((c for c in channel if c.chan_id == outgoing_chan_id), None)
        if outgoing_channel:
            db_channel = Channels.objects.filter(chan_id=outgoing_chan_id).first()
            if db_channel:
                old_local = db_channel.local_balance
                old_remote = db_channel.remote_balance
                db_channel.local_balance = outgoing_channel.local_balance
                db_channel.remote_balance = outgoing_channel.remote_balance
                db_channel.save()
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : update_channels: Outgoing chan {outgoing_chan_id} - local: {old_local}->{outgoing_channel.local_balance}, remote: {old_remote}->{outgoing_channel.remote_balance}")
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error updating channel balances: {str(e)}")

@sync_to_async
def auto_schedule() -> List[Rebalancer]:
    try:
        #No rebalancer jobs have been scheduled, lets look for any channels with an auto_rebalance flag and make the best request if we find one
        to_schedule = []
        enabled = get_local_setting('AR-Enabled', 0, int)
        if enabled == 0:
            return []
        
        # percent_outbound is computed on USABLE local liquidity (local_balance - local_chan_reserve)
        # so channels sitting right at their reserve floor don't qualify as sources.
        auto_rebalance_channels = Channels.objects.filter(is_active=True, is_open=True, private=False).annotate(percent_outbound=((Sum('local_balance')+Sum('pending_outbound')-Sum('local_chan_reserve'))*100)/Sum('capacity')).annotate(inbound_can=(((Sum('remote_balance')+Sum('pending_inbound'))*100)/Sum('capacity'))/Sum('ar_in_target'))
        if len(auto_rebalance_channels) == 0:
            return []
        
        get_local_setting('AR-Outbound%', 75, int)
        get_local_setting('AR-Inbound%', 90, int)
        active_pubkeys = get_active_rebalance_pubkeys()
        outbound_cans = list(
            auto_rebalance_channels
            .filter(percent_outbound__gte=F('ar_out_target'))
            .filter(
                Q(ar_source=True)        # explicitly enabled as source in Advanced Rebalancing
                | Q(auto_rebalance=False) # non-AR-managed channels are free to drain
            )
            .order_by('htlc_count')
            .values_list('chan_id', flat=True)
        )
        already_scheduled = (
            Rebalancer.objects.exclude(last_hop_pubkey='')
            .filter(status__in=[0, 1])
            .values_list('last_hop_pubkey', flat=True)
        )
        inbound_cans = (
            auto_rebalance_channels
            .filter(auto_rebalance=True, inbound_can__gte=1)
            .exclude(remote_pubkey__in=already_scheduled)
            .order_by('-inbound_can')
        )
        if len(inbound_cans) == 0 or len(outbound_cans) == 0:
            return []

        max_fee_rate = get_local_setting('AR-MaxFeeRate', 500, int)
        variance = get_local_setting('AR-Variance', 0, int)
        wait_period = get_local_setting('AR-WaitPeriod', 30, int)
        get_local_setting('AR-Target%', 3, int)
        get_local_setting('AR-MaxCost%', 65, int)
        source_fee_rate_map = dict(
            auto_rebalance_channels
            .filter(chan_id__in=outbound_cans)
            .values_list('chan_id', 'local_fee_rate')
        )
        min_source_fee_rate = min(source_fee_rate_map.values()) if source_fee_rate_map else 0
        allowed_map = {}
        for entry in AllowedTarget.objects.select_related('source_chan').all():
            allowed_map.setdefault(entry.source_chan.chan_id, []).append(entry.target_pubkey)

        inbound_list = list(inbound_cans)
        scheduled_targets = set()

        for source_id, pubs in allowed_map.items():
            if source_id not in outbound_cans:
                continue
            for pub in pubs:
                if pub in active_pubkeys or pub in already_scheduled:
                    continue
                target = next((c for c in inbound_list if c.remote_pubkey == pub), None)
                if not target:
                    continue
                source_fee_rate = source_fee_rate_map.get(source_id, 0)
                target_fee_rate = min(max_fee_rate, int(target.local_fee_rate * (target.ar_max_cost/100)) - source_fee_rate)
                if target_fee_rate <= target.remote_fee_rate:
                    continue
                target_value = int(target.ar_amt_target+(target.ar_amt_target*((secrets.choice(range(-1000,1001))/1000)*variance/100)))
                target_fee = round(target_fee_rate*target_value*0.000001, 3) if target_fee_rate <= max_fee_rate else round(max_fee_rate*target_value*0.000001, 3)
                if target_fee == 0:
                    continue
                target_time = get_local_setting('AR-Time', 5, int)
                if Rebalancer.objects.filter(last_hop_pubkey=pub).exclude(status=0).exists():
                    last_rebalance = Rebalancer.objects.filter(last_hop_pubkey=pub).exclude(status=0).order_by('-id')[0]
                    if not (last_rebalance.status == 2 or (last_rebalance.status > 2 and (int((datetime.now() - last_rebalance.stop).total_seconds() / 60) > wait_period)) or (last_rebalance.status == 1 and ((int((datetime.now() - last_rebalance.start).total_seconds() / 60) - last_rebalance.duration) > wait_period))):
                        continue
                new_rebalance = Rebalancer(value=target_value, fee_limit=target_fee, outgoing_chan_ids=str([source_id]), last_hop_pubkey=pub, target_alias=target.alias, duration=target_time)
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Creating Auto Rebalance Request for allowed target {pub} via {source_id}")
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Value: {target_value} / {target.ar_amt_target} | Fee: {target_fee} | Duration: {target_time}")
                src_alias = Channels.objects.filter(chan_id=str(source_id)).values_list('alias', flat=True).first() or "?"
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Request routing outbound via: [{source_id} ({src_alias})]")
                new_rebalance.save()
                to_schedule.append(new_rebalance)
                scheduled_targets.add(pub)

        for target in inbound_list:
            if target.remote_pubkey in scheduled_targets:
                continue
            target_fee_rate = min(max_fee_rate, int(target.local_fee_rate * (target.ar_max_cost/100)) - min_source_fee_rate)
            if target_fee_rate > 0 and target_fee_rate > target.remote_fee_rate:
                target_value = int(target.ar_amt_target+(target.ar_amt_target*((secrets.choice(range(-1000,1001))/1000)*variance/100)))
                target_fee = round(target_fee_rate*target_value*0.000001, 3) if target_fee_rate <= max_fee_rate else round(max_fee_rate*target_value*0.000001, 3)
                if target_fee == 0:
                    continue
            
                target_time = get_local_setting('AR-Time', 5, int)
                # TLDR: willing to pay 1 sat for every value_per_fee sats moved
                if Rebalancer.objects.filter(last_hop_pubkey=target.remote_pubkey).exclude(status=0).exists():
                    last_rebalance = Rebalancer.objects.filter(last_hop_pubkey=target.remote_pubkey).exclude(status=0).order_by('-id')[0]
                    if not (last_rebalance.status == 2 or (last_rebalance.status > 2 and (int((datetime.now() - last_rebalance.stop).total_seconds() / 60) > wait_period)) or (last_rebalance.status == 1 and ((int((datetime.now() - last_rebalance.start).total_seconds() / 60) - last_rebalance.duration) > wait_period))):
                        continue
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Creating Auto Rebalance Request for: {target.chan_id}")
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Value: {target_value} / {target.ar_amt_target} | Fee: {target_fee} | Duration: {target_time}")
                out_alias_map = dict(Channels.objects.filter(chan_id__in=[str(c) for c in outbound_cans]).values_list('chan_id', 'alias'))
                out_labels = [f"{c} ({out_alias_map.get(str(c)) or '?'})" for c in outbound_cans]
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Request routing outbound via: {out_labels}")
                new_rebalance = Rebalancer(value=target_value, fee_limit=target_fee, outgoing_chan_ids=str(outbound_cans).replace('\'', ''), last_hop_pubkey=target.remote_pubkey, target_alias=target.alias, duration=target_time)
                new_rebalance.save()
                to_schedule.append(new_rebalance)
        return to_schedule
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error scheduling rebalances: {str(e)}")
        return to_schedule

@sync_to_async
def auto_enable():
    try:
        enabled = get_local_setting('AR-Autopilot', 0, int)
        apdays = get_local_setting('AR-APDays', 7, int)
        if enabled == 1:
            lookup_channels=Channels.objects.filter(is_active=True, is_open=True, private=False)
            channels = lookup_channels.values('remote_pubkey').annotate(outbound_percent=((Sum('local_balance')+Sum('pending_outbound'))*1000)/Sum('capacity')).annotate(inbound_percent=((Sum('remote_balance')+Sum('pending_inbound'))*1000)/Sum('capacity')).order_by()
            filter_day = datetime.now() - timedelta(days=apdays)
            forwards = Forwards.objects.filter(forward_date__gte=filter_day)
            for channel in channels:
                outbound_percent = int(round(channel['outbound_percent']/10, 0))
                inbound_percent = int(round(channel['inbound_percent']/10, 0))
                chan_list = lookup_channels.filter(remote_pubkey=channel['remote_pubkey']).values('chan_id')
                routed_in_apday = forwards.filter(chan_id_in__in=chan_list).count()
                routed_out_apday = forwards.filter(chan_id_out__in=chan_list).count()
                iapD = 0 if routed_in_apday == 0 else int(forwards.filter(chan_id_in__in=chan_list).aggregate(Sum('amt_in_msat'))['amt_in_msat__sum']/10000000)/100
                oapD = 0 if routed_out_apday == 0 else int(forwards.filter(chan_id_out__in=chan_list).aggregate(Sum('amt_out_msat'))['amt_out_msat__sum']/10000000)/100
                for peer_channel in lookup_channels.filter(chan_id__in=chan_list):
                    if peer_channel.ar_out_target == 100 and peer_channel.auto_rebalance == True:
                        #Special Case for LOOP, Wos, etc. Always Auto Rebalance if enabled to keep outbound full.
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Skipping AR enabled and 100% oTarget channel: {peer_channel.alias} {peer_channel.chan_id}")
                        pass
                    elif oapD > (iapD*1.10) and outbound_percent > 75:
                        #print('Case 1: Pass')
                        pass
                    elif oapD > (iapD*1.10) and inbound_percent > 75 and peer_channel.auto_rebalance == False:
                        #print('Case 2: Enable AR - o7D > i7D AND Inbound Liq > 75%')
                        peer_channel.auto_rebalance = True
                        peer_channel.save()
                        Autopilot(chan_id=peer_channel.chan_id, peer_alias=peer_channel.alias, setting='Enabled', old_value=0, new_value=1).save()
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Auto Pilot Enabled for {peer_channel.alias} {peer_channel.chan_id}: {oapD} {iapD}")
                    elif oapD < (iapD*1.10) and outbound_percent > 75 and peer_channel.auto_rebalance == True:
                        #print('Case 3: Disable AR - o7D < i7D AND Outbound Liq > 75%')
                        peer_channel.auto_rebalance = False
                        peer_channel.save()
                        Autopilot(chan_id=peer_channel.chan_id, peer_alias=peer_channel.alias, setting='Enabled', old_value=1, new_value=0).save()
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Auto Pilot Disabled for {peer_channel.alias} {peer_channel.chan_id}: {oapD} {iapD}" )
                    elif oapD < (iapD*1.10) and inbound_percent > 75:
                        #print('Case 4: Pass')
                        pass
                    else:
                        #print('Case 5: Pass')
                        pass
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error during auto channel enabling: {str(e)}")

@sync_to_async
def get_pending_rebals():
    try:
        rebalances = Rebalancer.objects.filter(status=0).order_by('id')
        return rebalances, len(rebalances)
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting pending rebalances: {str(e)}")

async def async_queue_manager(rebalancer_queue):
    global scheduled_rebalances, active_rebalances, shutdown_rebalancer
    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Queue manager is starting...")
    try:
        while True:
            if shutdown_rebalancer == True:
                return
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Queue currently has {rebalancer_queue.qsize()} items...")
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : There are currently {len(active_rebalances)} tasks in progress...")
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Queue manager is checking for more work...")
            dispatch_interval = await get_dispatch_interval()
            pending_rebalances, rebal_count = await get_pending_rebals()
            if rebal_count > 0:
                for rebalance in pending_rebalances:
                    if rebalance.id not in (scheduled_rebalances + active_rebalances):
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Found a pending job to schedule with id: {rebalance.id}")
                        scheduled_rebalances.append(rebalance.id)
                        await rebalancer_queue.put(rebalance)
                        if dispatch_interval > 0:
                            await asyncio.sleep(dispatch_interval)
            await auto_enable()
            scheduled = await auto_schedule()
            if len(scheduled) > 0:
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Scheduling {len(scheduled)} more jobs...")
                for rebalance in scheduled:
                    scheduled_rebalances.append(rebalance.id)
                    await rebalancer_queue.put(rebalance)
                    if dispatch_interval > 0:
                        await asyncio.sleep(dispatch_interval)
            elif rebalancer_queue.qsize() == 0 and len(active_rebalances) == 0:
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Queue is still empty, stopping the rebalancer...")
                shutdown_rebalancer = True
                return
            await asyncio.sleep(30)
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Queue manager exception: {str(e)}")
        shutdown_rebalancer = True
    finally:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Queue manager has shut down...")

async def async_run_rebalancer(worker, rebalancer_queue):
    global scheduled_rebalances, active_rebalances, shutdown_rebalancer
    while True:
        if not rebalancer_queue.empty() and not shutdown_rebalancer:
            rebalance = await rebalancer_queue.get()
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : {worker} is starting a new request...")
            active_rebalance_id = None
            if rebalance != None:
                active_rebalance_id = rebalance.id
                active_rebalances.append(active_rebalance_id)
                scheduled_rebalances.remove(active_rebalance_id)
            while rebalance != None:
                rebalance = await run_rebalancer(rebalance, worker)
            if active_rebalance_id != None:
                active_rebalances.remove(active_rebalance_id)
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : {worker} completed its request...")
        else:
            if shutdown_rebalancer == True:
                return
        await asyncio.sleep(3)

async def start_queue(worker_count=1):
    rebalancer_queue = asyncio.Queue()
    manager = asyncio.create_task(async_queue_manager(rebalancer_queue))
    workers = [asyncio.create_task(async_run_rebalancer("Worker " + str(worker_num+1), rebalancer_queue)) for worker_num in range(worker_count)]
    await asyncio.gather(manager, *workers)
    print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Manager and workers have stopped...")

@sync_to_async
def get_worker_count():
    return get_local_setting('AR-Workers', 1, int)

@sync_to_async
def get_dispatch_interval():
    return get_local_setting('AR-DispatchInterval', 5, int)

async def update_worker_count():
    global worker_count, shutdown_rebalancer
    while True:
        updated_worker_count = await get_worker_count()
        if updated_worker_count != worker_count:
            worker_count = updated_worker_count
            shutdown_rebalancer = True
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : New worker count detected...restarting rebalancer")
        await asyncio.sleep(20)

def main():
    global scheduled_rebalances, active_rebalances, shutdown_rebalancer, worker_count
    worker_count = get_local_setting('AR-Workers', 1, int)
    try:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Rebalancer initializing...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(update_worker_count())
        while True:
            shutdown_rebalancer = False
            scheduled_rebalances = []
            active_rebalances = []
            if Rebalancer.objects.filter(status=1).exists():
                unknown_errors = Rebalancer.objects.filter(status=1)
                for unknown_error in unknown_errors:
                    unknown_error.status = 400
                    unknown_error.stop = datetime.now()
                    unknown_error.save()
            loop.run_until_complete(start_queue(worker_count))
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Rebalancer successfully exited...sleeping for 20 seconds")
            sleep(20)
    except Exception as e:
        error = str(e)
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Rebalancer loop error: {error}")
    finally:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Rebalancer loop has been terminated")

if __name__ == '__main__':
    main()
