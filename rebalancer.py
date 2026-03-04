import django, json, secrets, asyncio
from time import sleep
from asgiref.sync import sync_to_async
from django.db.models import Sum, F, Q
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

@sync_to_async
def get_out_cans(rebalance, auto_rebalance_channels):
    try:
        exclude_keys = {rebalance.last_hop_pubkey}
        result = list(
            auto_rebalance_channels
            .filter(percent_outbound__gte=F('ar_out_target'))
            .filter(
                Q(auto_rebalance=False)
                | Q(
                    ar_source=True,
                    local_fee_rate__lt=F('ar_source_ppm_diff')
                )
            )
            .exclude(remote_pubkey__in=exclude_keys)
            .order_by('htlc_count')
            .values_list('chan_id', flat=True)
        )
        if len(result) > 1:
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : get_out_cans: Found {len(result)} candidate channels (ordered by DB htlc_count): {result}")
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
        # Fetch more candidates than needed so reputation + diversity can re-rank
        fetch_limit = max(limit * 3, 30)
        candidates = list(
            RebalanceRoute.objects
            .filter(target_pubkey=pubkey, outgoing_chan_id__in=chan_ids)
            .filter(Q(last_failure__lt=cutoff) | Q(last_failure__isnull=True))
            .annotate(
                ratio=calc_success_ratio(F('success_count'), F('failure_count')),
                weighted_ratio=calc_weighted_ratio(F('success_count'), F('failure_count')),
            )
            .order_by('-weighted_ratio')[:fetch_limit]
        )
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

        return selected
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error getting saved routes: {str(e)}")
        return []

@sync_to_async
def update_route(pubkey, chan_id, route_hex, success=True):
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
            if parsed.total_amt_msat and parsed.total_fees_msat:
                route_obj.last_fee_ppm = (parsed.total_fees_msat / parsed.total_amt_msat) * 1000000
        else:
            route_obj.failure_count += 1
            route_obj.last_failure = now
        route_obj.save()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error updating route record: {str(e)}")

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
        cutoff = timezone.now() - timedelta(days=7)
        RebalanceRoute.objects.filter(Q(last_success__lt=cutoff) | Q(last_success__isnull=True)).delete()
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
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Chan {c.chan_id} has {htlc_count} pending HTLCs (peer: {c.remote_pubkey[:16]}...)")

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
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Peer {peer[:16]}... has {len(peer_chan_list)} channels - selected chan {selected[0]} with {selected[1]} HTLCs, excluded: {[f'{c[0]}({c[1]} HTLCs)' for c in sorted_peer_chans[1:]]}")
            else:
                # Single channel to this peer - include it
                filtered_ids.append(peer_chan_list[0][0])

        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Original list: {chan_ids}")
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : HTLC Filter: Filtered list: {filtered_ids} (removed {len(chan_ids) - len(filtered_ids)} channels)")

        return filtered_ids
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Error filtering channels by HTLC: {str(e)}")
        return chan_ids

async def run_rebalancer(rebalance, worker):
    try:
        # Check if LocalSetting LND-EnableMPP exists and set allow_mpp accordingly
        allow_multishards = await check_and_set_allow_multishards()  # Default value is True.
        max_parts = None if allow_multishards else 1  # Adjust max_parts based on the allow_multishards value
        #Reduce potential rebalance value in percent out to avoid going below AR-OUT-Target
        auto_rebalance_channels = Channels.objects.filter(is_active=True, is_open=True, private=False).annotate(percent_outbound=((Sum('local_balance')+Sum('pending_outbound')-rebalance.value)*100)/Sum('capacity')).annotate(inbound_can=(((Sum('remote_balance')+Sum('pending_inbound'))*100)/Sum('capacity'))/Sum('ar_in_target'))
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
            for sr in saved_routes:
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Trying saved route via {sr.outgoing_chan_id}")
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
                        f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute succeeded via {sr.outgoing_chan_id}"
                    )

                    if build.route.total_fees_msat > fee_limit_msat:
                        print(
                            f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute via {sr.outgoing_chan_id} exceeds fee limit"
                        )
                        rebuilt_hex = build.route.SerializeToString().hex()
                        await update_route(
                            rebalance.last_hop_pubkey,
                            sr.outgoing_chan_id,
                            rebuilt_hex,
                            False,
                        )
                        payment_response = None
                        continue

                    route_msg = build.route
                    rebuilt_hex = route_msg.SerializeToString().hex()
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
                                f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved route succeeded via {sr.outgoing_chan_id} - hash: {rebalance.payment_hash}"
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
                            f"{datetime.now().strftime('%c')} : [Rebalancer] : Saved route failed via {sr.outgoing_chan_id} - code: {reason} - detail: {detail} - failure_hop: {fsi}"
                        )
                        await update_route(
                            rebalance.last_hop_pubkey, sr.outgoing_chan_id, rebuilt_hex, False
                        )
                        await update_node_reputations(rebuilt_hex, False, fsi)
                except Exception as e:
                    print(
                        f"{datetime.now().strftime('%c')} : [Rebalancer] : BuildRoute failed via {sr.outgoing_chan_id} - {e}"
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

            if payment_response is None or payment_response.status != 1:
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
            close_shared_async_channel()
            rebalance.stop = datetime.now()
            await save_record(rebalance)
            print(f"{datetime.now().strftime('%c')} : [Rebalancer] : {worker} completed payment attempts for: {rebalance.payment_hash}")
            original_alias = rebalance.target_alias
            inc=1.21
            dec=2
            if rebalance.status ==2:
                if successful_in is not None and successful_out is not None:
                    await update_channels(stub, successful_in, successful_out)
                #Reduce potential rebalance value in percent out to avoid going below AR-OUT-Target
                auto_rebalance_channels = Channels.objects.filter(is_active=True, is_open=True, private=False).annotate(percent_outbound=((Sum('local_balance')+Sum('pending_outbound')-rebalance.value*inc)*100)/Sum('capacity')).annotate(inbound_can=(((Sum('remote_balance')+Sum('pending_inbound'))*100)/Sum('capacity'))/Sum('ar_in_target'))
                inbound_cans = auto_rebalance_channels.filter(remote_pubkey=rebalance.last_hop_pubkey).filter(auto_rebalance=True, inbound_can__gte=1)
                outbound_cans = await get_out_cans(rebalance, auto_rebalance_channels)
                if await inbound_cans_len(inbound_cans) > 0 and len(outbound_cans) > 0:
                    next_rebalance = Rebalancer(value=int(rebalance.value*inc), fee_limit=round(rebalance.fee_limit*inc, 3), outgoing_chan_ids=str(outbound_cans).replace('\'', ''), last_hop_pubkey=rebalance.last_hop_pubkey, target_alias=original_alias, duration=1)
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
                    next_rebalance = Rebalancer(value=int(next_value), fee_limit=round(rebalance.fee_limit/(rebalance.value/next_value), 3), outgoing_chan_ids=str(outbound_cans).replace('\'', ''), last_hop_pubkey=rebalance.last_hop_pubkey, target_alias=original_alias, duration=1)
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
        
        auto_rebalance_channels = Channels.objects.filter(is_active=True, is_open=True, private=False).annotate(percent_outbound=((Sum('local_balance')+Sum('pending_outbound'))*100)/Sum('capacity')).annotate(inbound_can=(((Sum('remote_balance')+Sum('pending_inbound'))*100)/Sum('capacity'))/Sum('ar_in_target'))
        if len(auto_rebalance_channels) == 0:
            return []
        
        get_local_setting('AR-Outbound%', 75, int)
        get_local_setting('AR-Inbound%', 90, int)
        active_pubkeys = get_active_rebalance_pubkeys()
        outbound_cans = list(
            auto_rebalance_channels
            .filter(percent_outbound__gte=F('ar_out_target'))
            .filter(
                Q(auto_rebalance=False)
                | Q(
                    ar_source=True,
                    local_fee_rate__lt=F('ar_source_ppm_diff')
                )
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
                target_fee_rate = min(max_fee_rate, int(target.local_fee_rate * (target.ar_max_cost/100)))
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
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Request routing outbound via: {[source_id]}")
                new_rebalance.save()
                to_schedule.append(new_rebalance)
                scheduled_targets.add(pub)

        for target in inbound_list:
            if target.remote_pubkey in scheduled_targets:
                continue
            target_fee_rate = min(max_fee_rate, int(target.local_fee_rate * (target.ar_max_cost/100)))
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
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Request routing outbound via: {outbound_cans}")
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
            pending_rebalances, rebal_count = await get_pending_rebals()
            if rebal_count > 0:
                for rebalance in pending_rebalances:
                    if rebalance.id not in (scheduled_rebalances + active_rebalances):
                        print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Found a pending job to schedule with id: {rebalance.id}")
                        scheduled_rebalances.append(rebalance.id)
                        await rebalancer_queue.put(rebalance)
            await auto_enable()
            scheduled = await auto_schedule()
            if len(scheduled) > 0:
                print(f"{datetime.now().strftime('%c')} : [Rebalancer] : Scheduling {len(scheduled)} more jobs...")
                for rebalance in scheduled:
                    scheduled_rebalances.append(rebalance.id)
                    await rebalancer_queue.put(rebalance)
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
