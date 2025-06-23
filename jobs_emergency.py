import django
from datetime import datetime
from os import environ

from gui.lnd_deps import lightning_pb2 as ln

environ['DJANGO_SETTINGS_MODULE'] = 'lndg.settings'
django.setup()

from gui.models import Channels, LocalSettings, Autofees


def emergency_forward_check(stub, chan_ids):
    """Apply emergency fee increases after a forward settles."""
    if not isinstance(chan_ids, list):
        chan_ids = [chan_ids]

    if LocalSettings.objects.filter(key='EP-Enabled').exists():
        if int(LocalSettings.objects.filter(key='EP-Enabled')[0].value) == 0:
            return
    else:
        LocalSettings(key='EP-Enabled', value='0').save()
        return

    default_threshold = int(LocalSettings.objects.filter(key='EP-LiveThreshold').first().value)
    default_inc = float(LocalSettings.objects.filter(key='EP-LiveIncreasePct').first().value)

    db_channels = Channels.objects.filter(chan_id__in=chan_ids, ep_enabled=True)
    if not db_channels:
        return

    live_map = {
        ch.chan_id: ch
        for ch in stub.ListChannels(ln.ListChannelsRequest()).channels
        if ch.chan_id in chan_ids
    }

    for db_ch in db_channels:
        live_ch = live_map.get(db_ch.chan_id)
        if not live_ch:
            continue

        threshold = db_ch.ep_live_threshold if db_ch.ep_live_threshold is not None else default_threshold
        inc_pct = db_ch.ep_live_inc_pct if db_ch.ep_live_inc_pct is not None else default_inc

        pending_out = sum(h.amount for h in live_ch.pending_htlcs if not h.incoming)
        percent = (live_ch.local_balance + pending_out) * 100 / live_ch.capacity if live_ch.capacity else 0

        if percent < threshold:
            new_rate = int(db_ch.local_fee_rate * (1 + inc_pct / 100))
            channel_point = ln.ChannelPoint(
                funding_txid_bytes=bytes.fromhex(db_ch.funding_txid),
                funding_txid_str=db_ch.funding_txid,
                output_index=db_ch.output_index,
            )
            try:
                stub.UpdateChannelPolicy(
                    ln.PolicyUpdateRequest(
                        chan_point=channel_point,
                        base_fee_msat=db_ch.local_base_fee,
                        fee_rate=(new_rate / 1_000_000),
                        time_lock_delta=db_ch.local_cltv,
                    )
                )
                Autofees(
                    chan_id=db_ch.chan_id,
                    peer_alias=db_ch.alias,
                    setting='EP-L',
                    old_value=db_ch.local_fee_rate,
                    new_value=new_rate,
                ).save()
                db_ch.local_fee_rate = new_rate
                db_ch.fees_updated = datetime.now()
                db_ch.ep_updated = datetime.now()
                db_ch.save()
            except Exception as e:
                print(
                    f"{datetime.now().strftime('%c')} : [Data] : Error updating live emergency fee for {db_ch.chan_id}: {str(e)}"
                )

