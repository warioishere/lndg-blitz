import django
from datetime import datetime
from gui.lnd_deps import router_pb2 as lnr
from gui.lnd_deps import router_pb2_grpc as lnrouter
from gui.lnd_deps import lightning_pb2 as ln
from gui.lnd_deps import lightning_pb2_grpc as lnrpc
from gui.lnd_deps.lnd_connect import get_shared_channel, close_shared_channel
from os import environ
from time import sleep
environ['DJANGO_SETTINGS_MODULE'] = 'lndg.settings'
django.setup()
from gui.models import Channels, FailedHTLCs
from jobs_emergency import emergency_forward_check

def main():
    while True:
        try:
            print(f"{datetime.now().strftime('%c')} : [HTLC] : Starting failed HTLC stream...")
            connection = get_shared_channel()
            routerstub = lnrouter.RouterStub(connection)
            lightning_stub = lnrpc.LightningStub(connection)
            all_forwards = {}
            for response in routerstub.SubscribeHtlcEvents(lnr.SubscribeHtlcEventsRequest()):
                if response.event_type == 3 and str(response.link_fail_event) != '':
                    in_chan_id = response.incoming_channel_id
                    out_chan_id = response.outgoing_channel_id
                    in_chan = Channels.objects.filter(chan_id=in_chan_id)[0] if Channels.objects.filter(chan_id=in_chan_id).exists() else None
                    out_chan = Channels.objects.filter(chan_id=out_chan_id)[0] if Channels.objects.filter(chan_id=out_chan_id).exists() else None
                    in_chan_alias = in_chan.alias if in_chan is not None else None
                    out_chan_alias = out_chan.alias if out_chan is not None else None
                    out_chan_liq = max(0, (out_chan.local_balance - out_chan.local_chan_reserve)) if out_chan is not None else None
                    out_chan_pending = out_chan.pending_outbound if out_chan is not None else None
                    amount = int(response.link_fail_event.info.outgoing_amt_msat/1000)
                    wire_failure = response.link_fail_event.wire_failure
                    failure_detail = response.link_fail_event.failure_detail
                    missed_fee = (response.link_fail_event.info.incoming_amt_msat - response.link_fail_event.info.outgoing_amt_msat)/1000
                    FailedHTLCs(amount=amount, chan_id_in=in_chan_id, chan_id_out=out_chan_id, chan_in_alias=in_chan_alias, chan_out_alias=out_chan_alias, chan_out_liq=out_chan_liq, chan_out_pending=out_chan_pending, wire_failure=wire_failure, failure_detail=failure_detail, missed_fee=missed_fee).save()
                elif response.event_type == 3 and str(response.forward_event) != '':
                    # Add forward_event
                    key = str(response.incoming_channel_id) + str(response.outgoing_channel_id) + str(response.incoming_htlc_id) + str(response.outgoing_htlc_id)
                    all_forwards[key] = response.forward_event
                elif response.event_type == 3 and str(response.settle_event) != '':
                    # Delete forward_event
                    key = str(response.incoming_channel_id) + str(response.outgoing_channel_id) + str(response.incoming_htlc_id) + str(response.outgoing_htlc_id)
                    if key in all_forwards.keys():
                        del all_forwards[key]
                        emergency_forward_check(lightning_stub, response.outgoing_channel_id)
                elif response.event_type == 3 and str(response.forward_fail_event) == '':
                    key = str(response.incoming_channel_id) + str(response.outgoing_channel_id) + str(response.incoming_htlc_id) + str(response.outgoing_htlc_id)
                    if key in all_forwards.keys():
                        forward_event = all_forwards[key]
                        in_chan_id = response.incoming_channel_id
                        out_chan_id = response.outgoing_channel_id
                        in_chan = Channels.objects.filter(chan_id=in_chan_id)[0] if Channels.objects.filter(chan_id=in_chan_id).exists() else None
                        out_chan = Channels.objects.filter(chan_id=out_chan_id)[0] if Channels.objects.filter(chan_id=out_chan_id).exists() else None
                        in_chan_alias = in_chan.alias if in_chan is not None else None
                        out_chan_alias = out_chan.alias if out_chan is not None else None
                        out_chan_liq = max(0, (out_chan.local_balance - out_chan.local_chan_reserve)) if out_chan is not None else None
                        out_chan_pending = out_chan.pending_outbound if out_chan is not None else None
                        amount = int(forward_event.info.incoming_amt_msat/1000)
                        wire_failure = 99
                        failure_detail = 99
                        missed_fee = (forward_event.info.incoming_amt_msat - forward_event.info.outgoing_amt_msat)/1000
                        FailedHTLCs(amount=amount, chan_id_in=in_chan_id, chan_id_out=out_chan_id, chan_in_alias=in_chan_alias, chan_out_alias=out_chan_alias, chan_out_liq=out_chan_liq, chan_out_pending=out_chan_pending, wire_failure=wire_failure, failure_detail=failure_detail, missed_fee=missed_fee).save()
                        del all_forwards[key]
        except Exception as e:
            print(f"{datetime.now().strftime('%c')} : [HTLC] : Error while running failed HTLC stream: {str(e)}")
        finally:
            close_shared_channel()
            # Drop broken Django DB connections too so the next ORM call reconnects.
            django.db.close_old_connections()
            sleep(20)

if __name__ == '__main__':
    main()
