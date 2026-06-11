import django
from datetime import datetime
from os import environ
from threading import Thread
from time import sleep, monotonic
environ['DJANGO_SETTINGS_MODULE'] = 'lndg.settings'
django.setup()

from gui.lnd_deps import lightning_pb2 as ln
from gui.lnd_deps import lightning_pb2_grpc as lnrpc
from gui.lnd_deps.lnd_connect import get_shared_channel, close_shared_channel
from gui.models import Channels, LocalSettings, Peers, GraphEvent, Rebalancer, GraphProbeLog
from gui.node_cache import get_node_info_cached
from jobs import probe_targets


def _get_setting(key, default):
    s = LocalSettings.objects.filter(key=key).first()
    return s.value if s else default


def _ensure_setting(key, default):
    """Return setting value, auto-creating it with the default if missing."""
    s = LocalSettings.objects.filter(key=key).first()
    if s:
        return s.value
    LocalSettings(key=key, value=default).save()
    return default


def _load_ar_targets():
    """Return set of remote_pubkeys from AR channels, minus GW-Exclude list."""
    excluded = set()
    raw = _get_setting('GW-Exclude', '')
    if raw:
        excluded = set(pk.strip() for pk in raw.split(',') if pk.strip())
    return set(
        Channels.objects.filter(is_open=True, auto_rebalance=True)
        .exclude(remote_pubkey__in=excluded)
        .values_list('remote_pubkey', flat=True)
    )


def _get_alias(pubkey, stub=None):
    """Resolve alias via Peers table, then node cache, then truncated pubkey."""
    p = Peers.objects.filter(pubkey=pubkey).values_list('alias', flat=True).first()
    if p:
        return p
    if stub:
        try:
            info = get_node_info_cached(pubkey, stub)
            if info.node.alias:
                return info.node.alias
        except Exception:
            pass
    return pubkey[:12]


def _trigger_probe(stub, target_pubkey, other_pubkey=None, other_fee_ppm=None, chan_id=None):
    """Run probe_targets for a single target pubkey.
    If routes are found, schedule a rebalance regardless of whether the
    channel has reached its ar_in_target% threshold.
    Returns number of new routes."""
    targets = Channels.objects.filter(is_open=True, auto_rebalance=True, remote_pubkey=target_pubkey)
    if not targets.exists():
        return 0
    ch = targets.first()
    # Skip if all target channels are already full (no remote balance to pull in)
    if not any(c.remote_balance > c.local_chan_reserve for c in targets):
        print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : {ch.alias} - skip, all channels full")
        return 0
    outbound_cans = list(
        Channels.objects.filter(is_open=True)
        .exclude(auto_rebalance=True, ar_source=False)
        .values_list('chan_id', flat=True)
    )
    source_fee_map = dict(
        Channels.objects.filter(chan_id__in=outbound_cans)
        .values_list('chan_id', 'local_fee_rate')
    )
    max_fee_rate = int(_get_setting('AR-MaxFeeRate', '500'))
    max_per_target = int(_get_setting('QR-MaxPerTarget', '5'))

    other_alias = _get_alias(other_pubkey, stub) if other_pubkey else '?'
    budget_ppm = int(ch.local_fee_rate * ch.ar_max_cost / 100)
    sources_tried = min(len(outbound_cans), max_per_target)
    print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Probing {ch.alias} (fee={ch.local_fee_rate} ppm) "
          f"triggered by new channel {chan_id} from {other_alias} (fee={other_fee_ppm} ppm)")
    print(f"{datetime.now().strftime('%c')} : [GraphWatcher] :   trying {sources_tried} of {len(outbound_cans)} outbound sources, "
          f"budget = {ch.local_fee_rate} * {ch.ar_max_cost}% = {budget_ppm} ppm")

    total_new, total_existing, total_errors, target_details = probe_targets(stub, targets, outbound_cans, source_fee_map, max_fee_rate, max_per_target)

    # Check which routes go via the new peer
    from gui.models import RebalanceRoute
    recent_routes = RebalanceRoute.objects.filter(target_pubkey=target_pubkey).order_by('-id')[:10]
    route_lines = []
    routes_via_new = 0
    for rr in recent_routes:
        hops = rr.route.split('-')
        via_new_peer = other_pubkey in hops if other_pubkey else False
        if via_new_peer:
            routes_via_new += 1
        fee_str = f"{int(rr.last_fee_ppm)} ppm" if rr.last_fee_ppm else "? ppm"
        marker = " ← via new peer!" if via_new_peer else ""
        line = f"{len(hops)} hops, {fee_str}, out={rr.outgoing_chan_id}{marker}"
        route_lines.append(line)
        print(f"{datetime.now().strftime('%c')} : [GraphWatcher] :   route: {line}")

    print(f"{datetime.now().strftime('%c')} : [GraphWatcher] :   result: {total_new} new, {total_existing} existing, {total_errors} errors, {routes_via_new} via new peer")

    scheduled = False
    if total_new + total_existing > 0:
        _schedule_rebalance(target_pubkey, targets, outbound_cans, source_fee_map, max_fee_rate)
        scheduled = True
    else:
        print(f"{datetime.now().strftime('%c')} : [GraphWatcher] :   no routes found, skipping rebalance")

    # Save probe log to DB
    GraphProbeLog.objects.create(
        target_pubkey=target_pubkey,
        target_alias=ch.alias,
        target_fee=ch.local_fee_rate,
        target_max_cost=ch.ar_max_cost,
        trigger_chan_id=chan_id or '',
        other_pubkey=other_pubkey or '',
        other_alias=other_alias,
        other_fee_ppm=other_fee_ppm,
        budget_ppm=budget_ppm,
        sources_tried=sources_tried,
        routes_new=total_new,
        routes_existing=total_existing,
        errors=total_errors,
        routes_via_new_peer=routes_via_new,
        rebalance_scheduled=scheduled,
        details='\n'.join(route_lines),
    )

    return total_new


def _schedule_rebalance(target_pubkey, targets, outbound_cans, source_fee_map, max_fee_rate):
    """Schedule a rebalance for the target, bypassing the ar_in_target% check.

    Normally auto_schedule() only rebalances when inbound% >= ar_in_target%.
    Here we schedule unconditionally because the graph watcher already found
    a viable route worth exploiting."""
    # Don't stack up jobs if one is already pending or in-flight for this target
    if Rebalancer.objects.filter(last_hop_pubkey=target_pubkey, status__in=[0, 1]).exists():
        return

    # Pick the first target channel for amount/fee calculation
    ch = targets.first()
    if not ch or ch.remote_balance <= ch.local_chan_reserve:
        return

    target_time = int(_get_setting('AR-Time', '5'))
    min_source_fee = min(source_fee_map.values()) if source_fee_map else 0
    fee_rate = min(max_fee_rate, int(ch.local_fee_rate * (ch.ar_max_cost / 100)) - min_source_fee)
    if fee_rate <= 0:
        return

    fee_limit = round(fee_rate * ch.ar_amt_target * 0.000001, 3)

    Rebalancer(
        value=ch.ar_amt_target,
        fee_limit=fee_limit,
        outgoing_chan_ids=str(outbound_cans).replace('\'', ''),
        last_hop_pubkey=target_pubkey,
        target_alias=ch.alias,
        duration=target_time,
    ).save()
    print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Scheduled rebalance for {ch.alias}: {ch.ar_amt_target} sats @ max {fee_rate} ppm (bypassing ar_in_target)")


def _dispatch_probe(stub, target_pk, target_alias, cid, event_type, last_probe_time, cooldown, other_pk=None, other_fee_ppm=None):
    """Run probe in a background thread to avoid blocking the stream."""
    try:
        routes_found = _trigger_probe(stub, target_pk, other_pubkey=other_pk, other_fee_ppm=other_fee_ppm, chan_id=cid)
        last_probe_time[target_pk] = monotonic()
        if routes_found:
            GraphEvent.objects.filter(
                target_pubkey=target_pk,
                chan_id=cid,
                event_type=event_type,
                probe_triggered=True,
                routes_found=0,
            ).order_by('-timestamp').update(routes_found=routes_found)
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Probe error for {target_alias}: {e}")


def _trim_events(keep=500):
    old_ids = GraphEvent.objects.order_by('-timestamp').values_list('id', flat=True)[keep:]
    if old_ids:
        GraphEvent.objects.filter(id__in=list(old_ids)).delete()


def main():
    event_count = 0
    while True:
        try:
            enabled = _ensure_setting('GW-Enabled', '0')
            if enabled != '1':
                sleep(30)
                continue

            print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Starting graph subscription...")
            connection = get_shared_channel()
            stub = lnrpc.LightningStub(connection)

            # Get current block height to distinguish new channels from old ones
            try:
                chain_height = stub.GetInfo(ln.GetInfoRequest()).block_height
            except Exception:
                chain_height = 0

            # Load AR targets
            ar_targets = _load_ar_targets()
            last_target_refresh = monotonic()
            cooldown = int(_ensure_setting('GW-Cooldown', '300'))
            last_probe_time = {}  # pubkey -> monotonic timestamp

            print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Watching {len(ar_targets)} AR target(s), chain height {chain_height}")

            # Deduplicate: track which (chan_id, direction) we've already processed
            chan_state = {}  # (chan_id, adv_pubkey) -> True

            for update in stub.SubscribeChannelGraph(ln.GraphTopologySubscription()):
                # Periodically refresh targets and settings
                now = monotonic()
                if now - last_target_refresh > 60:
                    if _get_setting('GW-Enabled', '0') != '1':
                        print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Disabled, stopping stream")
                        break
                    ar_targets = _load_ar_targets()
                    cooldown = int(_get_setting('GW-Cooldown', '300'))
                    last_target_refresh = now

                # Process channel updates
                for cu in update.channel_updates:
                    adv = cu.advertising_node
                    conn = cu.connecting_node
                    # Only care about updates involving our AR targets
                    target_pk = None
                    other_pk = None
                    if adv in ar_targets:
                        target_pk = adv
                        other_pk = conn
                    elif conn in ar_targets:
                        target_pk = conn
                        other_pk = adv
                    else:
                        continue

                    cid = str(cu.chan_id)
                    adv_fee_ppm = cu.routing_policy.fee_rate_milli_msat
                    adv_base_fee = cu.routing_policy.fee_base_msat
                    adv_disabled = cu.routing_policy.disabled
                    capacity = cu.capacity

                    # Only care about genuinely new channels — skip all
                    # fee updates and state changes on existing channels.
                    # LND chan_id format: (block_height << 40) | (tx_index << 16) | output_index
                    funding_height = int(cid) >> 40
                    channel_age = chain_height - funding_height if chain_height else None
                    if channel_age is None or channel_age >= 144:
                        continue

                    # Deduplicate: only fire once per (channel, direction)
                    state_key = (cid, adv)
                    if state_key in chan_state:
                        continue
                    chan_state[state_key] = True

                    event_type = 'new_channel'
                    print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : new_channel {cid} (height {funding_height}, age {channel_age} blocks)")

                    # We always want to show the other node's fee toward the target
                    # (the routing cost to reach our target through this channel).
                    if adv == other_pk:
                        # The other node advertised — its fee IS the routing fee to target
                        fee_ppm = adv_fee_ppm
                        base_fee = adv_base_fee
                        disabled = adv_disabled
                    else:
                        # The target advertised its own policy — look up the other
                        # node's fee for this channel via GetChanInfo
                        fee_ppm = adv_fee_ppm
                        base_fee = adv_base_fee
                        disabled = adv_disabled
                        try:
                            info = stub.GetChanInfo(ln.ChanInfoRequest(chan_id=int(cid)))
                            if info.node1_pub == other_pk:
                                fee_ppm = info.node1_policy.fee_rate_milli_msat
                                base_fee = info.node1_policy.fee_base_msat
                                disabled = info.node1_policy.disabled
                            elif info.node2_pub == other_pk:
                                fee_ppm = info.node2_policy.fee_rate_milli_msat
                                base_fee = info.node2_policy.fee_base_msat
                                disabled = info.node2_policy.disabled
                        except Exception:
                            pass  # Fall back to advertiser's fee if lookup fails

                    target_alias = _get_alias(target_pk, stub)
                    other_alias = _get_alias(other_pk, stub) if other_pk else ''

                    # Probe the new channel
                    probe_triggered = False
                    last_t = last_probe_time.get(target_pk, 0)
                    if monotonic() - last_t >= cooldown:
                        probe_triggered = True
                        # Mark cooldown immediately to prevent duplicate dispatches
                        last_probe_time[target_pk] = monotonic()
                        Thread(
                            target=_dispatch_probe,
                            args=(stub, target_pk, target_alias, cid, event_type, last_probe_time, cooldown, other_pk, fee_ppm),
                            daemon=True,
                        ).start()
                    else:
                        remaining = int(cooldown - (monotonic() - last_t))
                        print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : new_channel on {target_alias} ({cid}), cooldown {remaining}s remaining")

                    GraphEvent.objects.create(
                        event_type=event_type,
                        chan_id=cid,
                        capacity=capacity,
                        fee_ppm=fee_ppm,
                        base_fee_msat=base_fee,
                        target_pubkey=target_pk,
                        target_alias=target_alias,
                        other_node=other_pk or '',
                        other_alias=other_alias,
                        disabled=disabled,
                        probe_triggered=probe_triggered,
                        routes_found=0,
                        policy_node=adv,
                    )
                    event_count += 1

                # Process closed channels
                for cc in update.closed_chans:
                    cid = str(cc.chan_id)
                    capacity = cc.capacity
                    # Remove cached state for this channel
                    for key in [k for k in chan_state if k[0] == cid]:
                        chan_state.pop(key, None)
                    # Look up via GetChanInfo (may fail for already-removed channels)
                    target_pk = None
                    other_pk = None
                    try:
                        info = stub.GetChanInfo(ln.ChanInfoRequest(chan_id=int(cid)))
                        if info.node1_pub in ar_targets:
                            target_pk = info.node1_pub
                            other_pk = info.node2_pub
                        elif info.node2_pub in ar_targets:
                            target_pk = info.node2_pub
                            other_pk = info.node1_pub
                    except Exception:
                        pass

                    if not target_pk:
                        continue

                    target_alias = _get_alias(target_pk, stub)
                    other_alias = _get_alias(other_pk, stub) if other_pk else ''

                    print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : chan_closed {cid} involving {target_alias}")
                    GraphEvent.objects.create(
                        event_type='chan_closed',
                        chan_id=cid,
                        capacity=capacity,
                        fee_ppm=None,
                        base_fee_msat=0,
                        target_pubkey=target_pk,
                        target_alias=target_alias,
                        other_node=other_pk or '',
                        other_alias=other_alias,
                        disabled=False,
                        probe_triggered=False,
                        routes_found=0,
                        policy_node='',
                    )
                    event_count += 1

                # Trim events periodically, not on every update
                if event_count >= 50:
                    _trim_events()
                    event_count = 0

        except Exception as e:
            print(f"{datetime.now().strftime('%c')} : [GraphWatcher] : Error: {e}")
        finally:
            close_shared_channel()
            # Drop broken Django DB connections so the next ORM call reconnects.
            django.db.close_old_connections()
            sleep(20)


if __name__ == '__main__':
    main()
