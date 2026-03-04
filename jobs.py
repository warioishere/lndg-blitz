import django
from time import sleep
from django.db.models import Max, Sum, Avg, Count, Q
from django.db.models.functions import TruncDay
from datetime import datetime, timedelta
from gui.lnd_deps import lightning_pb2 as ln
from gui.lnd_deps import lightning_pb2_grpc as lnrpc
from gui.lnd_deps import signer_pb2 as lns
from gui.lnd_deps import signer_pb2_grpc as lnsigner
from gui.lnd_deps.lnd_connect import get_shared_channel, close_shared_channel
from lndg import settings
from os import environ
from requests import get
environ['DJANGO_SETTINGS_MODULE'] = 'lndg.settings'
django.setup()
from gui.models import Payments, PaymentHops, Invoices, Forwards, Channels, Peers, Onchain, Closures, Resolutions, PendingHTLCs, LocalSettings, FailedHTLCs, Autofees, InboundFeeLog, PendingChannels, HistFailedHTLC, PeerEvents, RebalanceRoute
from gui.node_cache import get_node_info_cached
import af
from jobs_emergency import emergency_forward_check

CHANNEL_UPDATE_FIELDS = [
    'remote_pubkey', 'short_chan_id', 'funding_txid', 'output_index', 'capacity',
    'local_balance', 'remote_balance', 'unsettled_balance', 'local_commit',
    'local_chan_reserve', 'num_updates', 'initiator', 'alias', 'total_sent',
    'total_received', 'private', 'pending_outbound', 'pending_inbound',
    'htlc_count', 'local_base_fee', 'local_fee_rate', 'local_inbound_base_fee',
    'local_inbound_fee_rate', 'inbound_offset', 'offset_updated', 'local_disabled',
    'local_cltv', 'local_min_htlc_msat', 'local_max_htlc_msat', 'remote_base_fee',
    'remote_fee_rate', 'remote_inbound_base_fee', 'remote_inbound_fee_rate',
    'remote_disabled', 'remote_cltv', 'remote_min_htlc_msat',
    'remote_max_htlc_msat', 'push_amt', 'close_address', 'is_active', 'is_open',
    'last_update', 'auto_rebalance', 'ar_amt_target', 'ar_in_target',
    'ar_out_target', 'ar_max_cost', 'ar_source', 'ar_source_ppm_diff',
    'fees_updated', 'auto_fees', 'notes'
]


def apply_channel_defaults(ch):
    """Replicate Channels.save default logic without saving."""
    if ch.auto_fees is None:
        if LocalSettings.objects.filter(key='AF-Enabled').exists():
            enabled = int(LocalSettings.objects.filter(key='AF-Enabled')[0].value)
        else:
            LocalSettings(key='AF-Enabled', value='0').save()
            enabled = 0
        ch.auto_fees = False if enabled == 0 else True
    if not ch.ar_out_target:
        if LocalSettings.objects.filter(key='AR-Outbound%').exists():
            outbound_setting = int(LocalSettings.objects.filter(key='AR-Outbound%')[0].value)
        else:
            LocalSettings(key='AR-Outbound%', value='75').save()
            outbound_setting = 75
        ch.ar_out_target = outbound_setting
    if not ch.ar_in_target:
        if LocalSettings.objects.filter(key='AR-Inbound%').exists():
            inbound_setting = int(LocalSettings.objects.filter(key='AR-Inbound%')[0].value)
        else:
            LocalSettings(key='AR-Inbound%', value='90').save()
            inbound_setting = 90
        ch.ar_in_target = inbound_setting
    if not ch.ar_amt_target:
        if LocalSettings.objects.filter(key='AR-Target%').exists():
            amt_setting = float(LocalSettings.objects.filter(key='AR-Target%')[0].value)
        else:
            LocalSettings(key='AR-Target%', value='3').save()
            amt_setting = 3
        ch.ar_amt_target = int((amt_setting/100) * ch.capacity)
    if not ch.ar_max_cost:
        if LocalSettings.objects.filter(key='AR-MaxCost%').exists():
            cost_setting = int(LocalSettings.objects.filter(key='AR-MaxCost%')[0].value)
        else:
            LocalSettings(key='AR-MaxCost%', value='65').save()
            cost_setting = 65
        ch.ar_max_cost = cost_setting


def update_payments(stub):
    self_pubkey = stub.GetInfo(ln.GetInfoRequest()).identity_pubkey
    inflight_payments = Payments.objects.filter(status=1).order_by('index')
    for payment in inflight_payments:
        payment_data = stub.ListPayments(ln.ListPaymentsRequest(include_incomplete=True, index_offset=payment.index-1, max_payments=1)).payments
        #Ignore inflight payments before 30 days
        if len(payment_data) > 0 and payment.payment_hash == payment_data[0].payment_hash and payment.creation_date > (datetime.now() - timedelta(days=30)):
            update_payment(stub, payment_data[0], self_pubkey)
        else:
            payment.status = 3
            payment.save()
    last_index = Payments.objects.aggregate(Max('index'))['index__max'] if Payments.objects.exists() else 0
    payments = stub.ListPayments(ln.ListPaymentsRequest(include_incomplete=True, index_offset=last_index, max_payments=100)).payments
    for payment in payments:
        try:
            new_payment = Payments(creation_date=datetime.fromtimestamp(payment.creation_date), payment_hash=payment.payment_hash, value=round(payment.value_msat/1000, 3), fee=round(payment.fee_msat/1000, 3), status=payment.status, index=payment.payment_index)
            new_payment.save()
        except Exception as e:
            #Error inserting, try to update instead
            print(f"{datetime.now().strftime('%c')} : [Data] : Error processing {new_payment}: {str(e)}")
        update_payment(stub, payment, self_pubkey)

def update_payment(stub, payment, self_pubkey):
    db_payment = Payments.objects.filter(payment_hash=payment.payment_hash)[0]
    db_payment.creation_date = datetime.fromtimestamp(payment.creation_date)
    db_payment.value = round(payment.value_msat/1000, 3)
    db_payment.fee = round(payment.fee_msat/1000, 3)
    db_payment.status = payment.status
    db_payment.index = payment.payment_index
    if payment.status == 2 or payment.status == 1:
        PaymentHops.objects.filter(payment_hash=db_payment).delete()
        db_payment.chan_out = None
        db_payment.rebal_chan = None
        db_payment.save()
        for attempt in payment.htlcs:
            if attempt.status == 1 or attempt.status == 0:
                hops = attempt.route.hops
                hop_count = 0
                cost_to = 0
                total_hops = len(hops)
                for hop in hops:
                    hop_count += 1
                    try:
                        alias = get_node_info_cached(hop.pub_key, stub).node.alias
                    except Exception:
                        alias = ''
                    fee = hop.fee_msat/1000
                    if hop_count == total_hops:
                        # Add additional HTLC information in last hop alias
                        alias += f'[ {payment.status}-{attempt.status}-{attempt.failure.code}-{attempt.failure.failure_source_index} ]'
                    if attempt.status == 1 or attempt.status == 0 or (attempt.status == 2 and attempt.failure.code in (1,2,12)):
                        PaymentHops(payment_hash=db_payment, attempt_id=attempt.attempt_id, step=hop_count, chan_id=hop.chan_id, alias=alias, chan_capacity=hop.chan_capacity, node_pubkey=hop.pub_key, amt=round(hop.amt_to_forward_msat/1000, 3), fee=round(fee, 3), cost_to=round(cost_to, 3)).save()
                    cost_to += fee
                    if hop_count == 1 and attempt.status == 1:
                        if db_payment.chan_out is None:
                            db_payment.chan_out = hop.chan_id
                            db_payment.chan_out_alias = alias
                        else:
                            db_payment.chan_out = 'MPP'
                            db_payment.chan_out_alias = 'MPP'
                    if hop_count == total_hops and 5482373484 in hop.custom_records and db_payment.keysend_preimage is None:
                        records = hop.custom_records
                        message = records[34349334].decode('utf-8', errors='ignore')[:1000] if 34349334 in records else None
                        db_payment.keysend_preimage = records[5482373484].hex()
                        db_payment.message = message
                    if hop_count == total_hops and hop.pub_key == self_pubkey and db_payment.rebal_chan is None:
                        db_payment.rebal_chan = hop.chan_id
    db_payment.save()

def update_invoices(stub):
    open_invoices = Invoices.objects.filter(state=0).order_by('index')
    for open_invoice in open_invoices:
        invoice_data = stub.ListInvoices(ln.ListInvoiceRequest(index_offset=open_invoice.index-1, num_max_invoices=1)).invoices
        if len(invoice_data) > 0 and open_invoice.r_hash == invoice_data[0].r_hash.hex():
            update_invoice(stub, invoice_data[0], open_invoice)
        else:
            open_invoice.state = 2
            open_invoice.save()
    last_index = Invoices.objects.aggregate(Max('index'))['index__max'] if Invoices.objects.exists() else 0
    invoices = stub.ListInvoices(ln.ListInvoiceRequest(index_offset=last_index, num_max_invoices=100)).invoices
    for invoice in invoices:
        db_invoice = Invoices(creation_date=datetime.fromtimestamp(invoice.creation_date), r_hash=invoice.r_hash.hex(), value=round(invoice.value_msat/1000, 3), amt_paid=invoice.amt_paid_sat, state=invoice.state, index=invoice.add_index)
        db_invoice.save()
        update_invoice(stub, invoice, db_invoice)

def update_invoice(stub, invoice, db_invoice):
    if invoice.state == 1:
        if len(invoice.htlcs) > 0:
            chan_in_id = invoice.htlcs[0].chan_id
            alias = Channels.objects.filter(chan_id=chan_in_id)[0].alias if Channels.objects.filter(chan_id=chan_in_id).exists() else None
            records = invoice.htlcs[0].custom_records
            keysend_preimage = records[5482373484].hex() if 5482373484 in records else None
            message = records[34349334].decode('utf-8', errors='ignore')[:1000] if 34349334 in records else None
            if 34349337 in records and 34349339 in records and 34349343 in records and 34349334 in records:
                signerstub = lnsigner.SignerStub(lnd_connect())
                self_pubkey = stub.GetInfo(ln.GetInfoRequest()).identity_pubkey
                try:
                    valid = signerstub.VerifyMessage(lns.VerifyMessageReq(msg=(records[34349339]+bytes.fromhex(self_pubkey)+records[34349343]+records[34349334]), signature=records[34349337], pubkey=records[34349339])).valid
                except:
                    print(f"{datetime.now().strftime('%c')} : [Data] : Unable to validate signature on invoice: {invoice.r_hash.hex()}")
                    valid = False
                sender = records[34349339].hex() if valid == True else None
                try:
                    sender_alias = get_node_info_cached(sender, stub).node.alias if sender is not None else None
                except Exception:
                    sender_alias = None
            else:
                sender = None
                sender_alias = None
        else:
            chan_in_id = None
            alias = None
            keysend_preimage = None
            message = None
            sender = None
            sender_alias = None
        db_invoice.state = invoice.state
        db_invoice.amt_paid = invoice.amt_paid_sat
        db_invoice.settle_date = datetime.fromtimestamp(invoice.settle_date)
        db_invoice.chan_in = chan_in_id
        db_invoice.chan_in_alias = alias
        db_invoice.keysend_preimage = keysend_preimage
        db_invoice.message = message
        db_invoice.sender = sender
        db_invoice.sender_alias = sender_alias
    else:
        db_invoice.state = invoice.state
    db_invoice.save()

def update_forwards(stub):
    latest_forward = Forwards.objects.order_by('-forward_date', '-id').first()
    start_time = int(latest_forward.forward_date.timestamp()) if latest_forward else 1420070400
    processed_count = Forwards.objects.filter(forward_date=latest_forward.forward_date).count() if latest_forward else 0
    response = stub.ForwardingHistory(ln.ForwardingHistoryRequest(
        start_time=start_time,
        index_offset=processed_count,
        num_max_events=1000
    ))
    forwards = response.forwarding_events
    if not forwards:
        return

    chan_ids = set()
    for f in forwards:
        chan_ids.add(str(f.chan_id_in))
        chan_ids.add(str(f.chan_id_out))

    chan_map = {c.chan_id: c for c in Channels.objects.filter(chan_id__in=chan_ids)}

    new_forwards = []
    for forward in forwards:
        inbound_channel = chan_map.get(str(forward.chan_id_in))
        outbound_channel = chan_map.get(str(forward.chan_id_out))
        forward_datetime = datetime.fromtimestamp(forward.timestamp)
        amt_in_msat = forward.amt_in_msat
        amt_out_msat = forward.amt_out_msat
        in_fee_msat = 0
        if outbound_channel and outbound_channel.fees_updated < forward_datetime:
            out_fee_msat = int((amt_out_msat * (outbound_channel.local_fee_rate / 1000000)) + outbound_channel.local_base_fee)
            if forward.fee_msat < out_fee_msat:
                in_fee_msat = out_fee_msat - forward.fee_msat
        incoming_peer_alias = (inbound_channel.remote_pubkey[:12] if inbound_channel.alias == '' else inbound_channel.alias) if inbound_channel else forward.peer_alias_in
        outgoing_peer_alias = (outbound_channel.remote_pubkey[:12] if outbound_channel.alias == '' else outbound_channel.alias) if outbound_channel else forward.peer_alias_out
        new_forwards.append(Forwards(
            forward_date=forward_datetime,
            chan_id_in=forward.chan_id_in,
            chan_id_out=forward.chan_id_out,
            chan_in_alias=incoming_peer_alias,
            chan_out_alias=outgoing_peer_alias,
            amt_in_msat=amt_in_msat,
            amt_out_msat=amt_out_msat,
            fee=round(forward.fee_msat / 1000, 3),
            inbound_fee=round(in_fee_msat / 1000, 3)
        ))
    Forwards.objects.bulk_create(new_forwards)
    emergency_forward_check(stub, [f.chan_id_out for f in forwards])

def disconnectpeer(stub, peer):
    try:
        stub.DisconnectPeer(ln.DisconnectPeerRequest(pub_key=peer.pubkey))
        print(f"{datetime.now().strftime('%c')} : [Data] : Disconnected peer {peer.alias} {peer.pubkey}")
        peer.connected = False
        peer.save()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Data] : Error disconnecting peer {peer.alias} {peer.pubkey}: {str(e)}")

def update_channels(stub):
    counter = 0
    chan_list = []
    channels_to_create = []
    channels_to_update = []
    pending_htlcs_to_create = []
    channels = stub.ListChannels(ln.ListChannelsRequest()).channels
    PendingHTLCs.objects.all().delete()
    get_info = stub.GetInfo(ln.GetInfoRequest())
    block_height = get_info.block_height
    version = get_info.version
    for channel in channels:
        is_new = False
        if Channels.objects.filter(chan_id=channel.chan_id).exists():
            db_channel = Channels.objects.filter(chan_id=channel.chan_id)[0]
            pending_channel = None
            peer_alias = Peers.objects.filter(pubkey=channel.remote_pubkey).values_list('alias', flat=True).first()
            if peer_alias is not None and peer_alias != db_channel.alias:
                db_channel.alias = peer_alias
        else:
            is_new = True
            try:
                alias = get_node_info_cached(channel.remote_pubkey, stub).node.alias
            except Exception:
                alias = ''
            channel_point = channel.channel_point
            txid, index = channel_point.split(':')
            db_channel = Channels(
                remote_pubkey=channel.remote_pubkey,
                chan_id=channel.chan_id,
                short_chan_id=str(channel.chan_id >> 40) + 'x' + str(channel.chan_id >> 16 & 0xFFFFFF) + 'x' + str(channel.chan_id & 0xFFFF),
                initiator=channel.initiator,
                alias=alias,
                funding_txid=txid,
                output_index=index,
                capacity=channel.capacity,
                private=channel.private,
                push_amt=channel.push_amount_sat,
                close_address=channel.close_address,
            )
            pending_channel = PendingChannels.objects.filter(funding_txid=txid, output_index=index).first()
        # Update basic channel data
        db_channel.local_balance = channel.local_balance
        db_channel.remote_balance = channel.remote_balance
        db_channel.unsettled_balance = channel.unsettled_balance
        db_channel.local_commit = channel.commit_fee
        db_channel.local_chan_reserve = channel.local_chan_reserve_sat
        db_channel.num_updates = channel.num_updates
        db_channel.is_open = True
        db_channel.total_sent = channel.total_satoshis_sent
        db_channel.total_received = channel.total_satoshis_received
        pending_out = 0
        pending_in = 0
        htlc_counter = 0
        if len(channel.pending_htlcs) > 0:
            for htlc in channel.pending_htlcs:
                pending_htlc = PendingHTLCs()
                pending_htlc.chan_id = db_channel.chan_id
                pending_htlc.alias = db_channel.alias
                pending_htlc.incoming = htlc.incoming
                pending_htlc.amount = htlc.amount
                pending_htlc.hash_lock = htlc.hash_lock.hex()
                pending_htlc.expiration_height = htlc.expiration_height
                pending_htlc.forwarding_channel = htlc.forwarding_channel
                pending_htlc.forwarding_alias = Channels.objects.filter(chan_id=htlc.forwarding_channel)[0].alias if Channels.objects.filter(chan_id=htlc.forwarding_channel).exists() else '---'
                pending_htlcs_to_create.append(pending_htlc)
                if htlc.incoming == True:
                    pending_in += htlc.amount
                else:
                    pending_out += htlc.amount
                htlc_counter += 1
                if htlc.expiration_height - block_height <= 13: # If htlc is expiring within 13 blocks, disconnect peer to help resolve the stuck htlc
                    peer = Peers.objects.filter(pubkey=channel.remote_pubkey)[0] if Peers.objects.filter(pubkey=channel.remote_pubkey).exists() else None
                    if peer and (not peer.last_reconnected or (int((datetime.now() - peer.last_reconnected).total_seconds() / 60) > 10)):
                        print(f"{datetime.now().strftime('%c')} : [Data] : HTLC expiring at {htlc.expiration_height} and within 13 blocks of {block_height}, disconnecting peer {channel.remote_pubkey} to resolve HTLC: {htlc.hash_lock.hex()} ")
                        disconnectpeer(stub, peer)
                        peer.last_reconnected = datetime.now()
                        peer.save()
                    else:
                        print(f"{datetime.now().strftime('%c')} : [Data] : Could not find peer {channel.remote_pubkey} with expiring HTLC: {htlc.hash_lock.hex()}")
        db_channel.pending_outbound = pending_out
        db_channel.pending_inbound = pending_in
        db_channel.htlc_count = htlc_counter
        # Check for peer events
        if db_channel.is_active != channel.active:
            db_channel.last_update = datetime.now()
            peer_alias = Peers.objects.filter(pubkey=db_channel.remote_pubkey)[0].alias if Peers.objects.filter(pubkey=db_channel.remote_pubkey).exists() else None
            db_channel.alias = '' if peer_alias is None else peer_alias
            if db_channel.is_active is None:
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Connection', old_value=None, new_value=(1 if channel.active else 0), out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
            elif channel.active:
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Connection', old_value=0, new_value=1, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
            else:
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Connection', old_value=1, new_value=0, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
            db_channel.is_active = channel.active
        try:
            chan_data = stub.GetChanInfo(ln.ChanInfoRequest(chan_id=channel.chan_id))
            if chan_data.node1_pub == channel.remote_pubkey:
                local_policy = chan_data.node2_policy
                remote_policy = chan_data.node1_policy
            else:
                local_policy = chan_data.node1_policy
                remote_policy = chan_data.node2_policy
            old_fee_rate = db_channel.local_fee_rate if db_channel.local_fee_rate is not None else 0
            db_channel.local_base_fee = local_policy.fee_base_msat
            db_channel.local_fee_rate = local_policy.fee_rate_milli_msat
            db_channel.local_cltv = local_policy.time_lock_delta
            db_channel.local_disabled = local_policy.disabled
            db_channel.local_min_htlc_msat = local_policy.min_htlc
            db_channel.local_max_htlc_msat = local_policy.max_htlc_msat
            if float(version[:4]) >= 0.18:
                try:
                    db_channel.local_inbound_base_fee = local_policy.inbound_fee_base_msat
                    db_channel.local_inbound_fee_rate = local_policy.inbound_fee_rate_milli_msat
                except:
                    db_channel.local_inbound_base_fee = 0
                    db_channel.local_inbound_fee_rate = 0
            else:
                db_channel.local_inbound_base_fee = 0
                db_channel.local_inbound_fee_rate = 0
            if db_channel.remote_cltv == -1:
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='BaseFee', old_value=None, new_value=remote_policy.fee_base_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                db_channel.remote_base_fee = remote_policy.fee_base_msat
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='FeeRate', old_value=None, new_value=remote_policy.fee_rate_milli_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                db_channel.remote_fee_rate = remote_policy.fee_rate_milli_msat
                if remote_policy.disabled:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Disabled', old_value=None, new_value=1, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                else:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Disabled', old_value=None, new_value=0, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                db_channel.remote_disabled = remote_policy.disabled
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='CLTV', old_value=None, new_value=remote_policy.time_lock_delta, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                db_channel.remote_cltv = remote_policy.time_lock_delta
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='MinHTLC', old_value=None, new_value=remote_policy.min_htlc, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                db_channel.remote_min_htlc_msat = remote_policy.min_htlc
                PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='MaxHTLC', old_value=None, new_value=remote_policy.max_htlc_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                db_channel.remote_max_htlc_msat = remote_policy.max_htlc_msat
                if float(version[:4]) >= 0.18:
                    try:
                        PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='IncomingBaseFee', old_value=None, new_value=remote_policy.inbound_fee_base_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                        db_channel.remote_inbound_base_fee = remote_policy.inbound_fee_base_msat
                        PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='IncomingFeeRate', old_value=None, new_value=remote_policy.inbound_fee_rate_milli_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                        db_channel.remote_inbound_fee_rate = remote_policy.inbound_fee_rate_milli_msat
                    except:
                        db_channel.remote_inbound_base_fee = 0
                        db_channel.remote_inbound_fee_rate = 0
                else:
                    db_channel.remote_inbound_base_fee = 0
                    db_channel.remote_inbound_fee_rate = 0
            else:
                if db_channel.remote_base_fee != remote_policy.fee_base_msat:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='BaseFee', old_value=db_channel.remote_base_fee, new_value=remote_policy.fee_base_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    db_channel.remote_base_fee = remote_policy.fee_base_msat
                if db_channel.remote_fee_rate != remote_policy.fee_rate_milli_msat:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='FeeRate', old_value=db_channel.remote_fee_rate, new_value=remote_policy.fee_rate_milli_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    db_channel.remote_fee_rate = remote_policy.fee_rate_milli_msat
                if db_channel.remote_disabled != remote_policy.disabled:
                    if db_channel.remote_disabled is None:
                        PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Disabled', old_value=None, new_value=(1 if remote_policy.disabled else 0), out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    elif remote_policy.disabled:
                        PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Disabled', old_value=0, new_value=1, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    else:
                        PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='Disabled', old_value=1, new_value=0, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    db_channel.remote_disabled = remote_policy.disabled
                if db_channel.remote_cltv != remote_policy.time_lock_delta:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='CLTV', old_value=db_channel.remote_cltv, new_value=remote_policy.time_lock_delta, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    db_channel.remote_cltv = remote_policy.time_lock_delta
                if db_channel.remote_min_htlc_msat != remote_policy.min_htlc:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='MinHTLC', old_value=db_channel.remote_min_htlc_msat, new_value=remote_policy.min_htlc, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    db_channel.remote_min_htlc_msat = remote_policy.min_htlc
                if db_channel.remote_max_htlc_msat != remote_policy.max_htlc_msat:
                    PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='MaxHTLC', old_value=db_channel.remote_max_htlc_msat, new_value=remote_policy.max_htlc_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                    db_channel.remote_max_htlc_msat = remote_policy.max_htlc_msat
                if float(version[:4]) >= 0.18:
                    if db_channel.remote_inbound_base_fee != remote_policy.inbound_fee_base_msat:
                        try:
                            PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='IncomingBaseFee', old_value=db_channel.remote_inbound_base_fee, new_value=remote_policy.inbound_fee_base_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                            db_channel.remote_inbound_base_fee = remote_policy.inbound_fee_base_msat
                        except:
                            db_channel.remote_inbound_base_fee = 0
                    if db_channel.remote_inbound_fee_rate != remote_policy.inbound_fee_rate_milli_msat:
                        try:
                            PeerEvents(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, event='IncomingFeeRate', old_value=db_channel.remote_inbound_fee_rate, new_value=remote_policy.inbound_fee_rate_milli_msat, out_liq=(db_channel.local_balance + db_channel.pending_outbound)).save()
                            db_channel.remote_inbound_fee_rate = remote_policy.inbound_fee_rate_milli_msat
                        except:
                            db_channel.remote_inbound_fee_rate = 0
                else:
                    db_channel.remote_inbound_base_fee = 0
                    db_channel.remote_inbound_fee_rate = 0
        except Exception as e: # LND has not found the channel on the graph
            print(f"{datetime.now().strftime('%c')} : [Data] : Error getting graph data for channel {db_channel.chan_id}: {str(e)}")
            if pending_channel: # skip adding new channel to the list, LND may not have added to the graph yet
                print(f"{datetime.now().strftime('%c')} : [Data] : Waiting for pending channel {db_channel.chan_id} to be added to the graph...")
                continue
            else:
                old_fee_rate = None
                db_channel.local_base_fee = -1 if db_channel.local_base_fee is None else db_channel.local_base_fee
                db_channel.local_fee_rate = -1 if db_channel.local_fee_rate is None else db_channel.local_fee_rate
                db_channel.local_cltv = -1 if db_channel.local_cltv is None else db_channel.local_cltv
                db_channel.local_disabled = False if db_channel.local_disabled is None else db_channel.local_disabled
                db_channel.local_min_htlc_msat = -1 if db_channel.local_min_htlc_msat is None else db_channel.local_min_htlc_msat
                db_channel.local_max_htlc_msat = -1 if db_channel.local_max_htlc_msat is None else db_channel.local_max_htlc_msat
                db_channel.remote_base_fee = -1 if db_channel.remote_base_fee is None else db_channel.remote_base_fee
                db_channel.remote_fee_rate = -1 if db_channel.remote_fee_rate is None else db_channel.remote_fee_rate
                db_channel.remote_cltv = -1 if db_channel.remote_cltv is None else db_channel.remote_cltv
                db_channel.remote_disabled = False if db_channel.remote_disabled is None else db_channel.remote_disabled
                db_channel.remote_min_htlc_msat = -1 if db_channel.remote_min_htlc_msat is None else db_channel.remote_min_htlc_msat
                db_channel.remote_max_htlc_msat = -1 if db_channel.remote_max_htlc_msat is None else db_channel.remote_max_htlc_msat
                db_channel.local_inbound_base_fee = -1 if db_channel.local_inbound_base_fee is None else db_channel.local_inbound_base_fee
                db_channel.local_inbound_fee_rate = -1 if db_channel.local_inbound_fee_rate is None else db_channel.local_inbound_fee_rate
                db_channel.remote_inbound_base_fee = -1 if db_channel.remote_inbound_base_fee is None else db_channel.remote_inbound_base_fee
                db_channel.remote_inbound_fee_rate = -1 if db_channel.remote_inbound_fee_rate is None else db_channel.remote_inbound_fee_rate
        # Check for pending settings to be applied
        if pending_channel:
            if pending_channel.local_base_fee or pending_channel.local_fee_rate or pending_channel.local_cltv:
                base_fee = pending_channel.local_base_fee if pending_channel.local_base_fee else db_channel.local_base_fee
                fee_rate = pending_channel.local_fee_rate if pending_channel.local_fee_rate else db_channel.local_fee_rate
                cltv = pending_channel.local_cltv if pending_channel.local_cltv else db_channel.local_cltv
                channel_point = ln.ChannelPoint()
                channel_point.funding_txid_bytes = bytes.fromhex(db_channel.funding_txid)
                channel_point.funding_txid_str = db_channel.funding_txid
                channel_point.output_index = int(db_channel.output_index)
                stub.UpdateChannelPolicy(ln.PolicyUpdateRequest(chan_point=channel_point, base_fee_msat=base_fee, fee_rate=(fee_rate/1000000), time_lock_delta=cltv))
                db_channel.local_base_fee = base_fee
                db_channel.local_fee_rate = fee_rate
                db_channel.local_cltv = cltv
                db_channel.fees_updated = datetime.now()
            if pending_channel.auto_rebalance is not None:
                db_channel.auto_rebalance = pending_channel.auto_rebalance
            if pending_channel.ar_amt_target:
                db_channel.ar_amt_target = pending_channel.ar_amt_target
            if pending_channel.ar_in_target:
                db_channel.ar_in_target = pending_channel.ar_in_target
            if pending_channel.ar_out_target:
                db_channel.ar_out_target = pending_channel.ar_out_target
            if pending_channel.ar_max_cost:
                db_channel.ar_max_cost = pending_channel.ar_max_cost
            if pending_channel.auto_fees is not None:
                db_channel.auto_fees = pending_channel.auto_fees
            pending_channel.delete()
        if old_fee_rate is not None and old_fee_rate != local_policy.fee_rate_milli_msat:
            print(f"{datetime.now().strftime('%c')} : [Data] : Ext fee change detected on {db_channel.chan_id} for peer {db_channel.alias}: fee updated from {old_fee_rate} to {db_channel.local_fee_rate}")
            #External Fee change detected, update auto fee log
            db_channel.fees_updated = datetime.now()
            Autofees(chan_id=db_channel.chan_id, peer_alias=db_channel.alias, setting=(f"Ext"), old_value=old_fee_rate, new_value=db_channel.local_fee_rate).save()
        apply_channel_defaults(db_channel)
        if is_new:
            channels_to_create.append(db_channel)
        else:
            channels_to_update.append(db_channel)
        counter += 1
        chan_list.append(channel.chan_id)
    records = Channels.objects.filter(is_open=True).count()
    if records > counter:
        channels = list(Channels.objects.filter(is_open=True).exclude(chan_id__in=chan_list))
        for channel in channels:
            channel.last_update = datetime.now()
            channel.is_active = False
            channel.is_open = False
            apply_channel_defaults(channel)
        channels_to_update.extend(channels)

    if pending_htlcs_to_create:
        PendingHTLCs.objects.bulk_create(pending_htlcs_to_create)
    if channels_to_create:
        Channels.objects.bulk_create(channels_to_create)
    if channels_to_update:
        Channels.objects.bulk_update(channels_to_update, CHANNEL_UPDATE_FIELDS)

def update_peers(stub):
    peer_list = []
    peers_to_create = []
    peers_to_update = []
    peers = stub.ListPeers(ln.ListPeersRequest(latest_error=True)).peers
    for peer in peers:
        db_peer = Peers.objects.filter(pubkey=peer.pub_key).first()
        if db_peer:
            db_peer.address = peer.address
            db_peer.sat_sent = peer.sat_sent
            db_peer.sat_recv = peer.sat_recv
            db_peer.inbound = peer.inbound
            db_peer.ping_time = round(peer.ping_time/1000)
            try:
                alias = get_node_info_cached(peer.pub_key, stub).node.alias
            except Exception:
                alias = None
            if alias and (not db_peer.alias or db_peer.alias != alias):
                db_peer.alias = alias
            db_peer.connected = True
            peers_to_update.append(db_peer)
        else:
            try:
                alias = get_node_info_cached(peer.pub_key, stub).node.alias
            except Exception:
                alias = ''
            peers_to_create.append(Peers(pubkey=peer.pub_key, address=peer.address, sat_sent=peer.sat_sent, sat_recv=peer.sat_recv, inbound=peer.inbound, ping_time=round(peer.ping_time/1000), alias=alias, connected=True))
        peer_list.append(peer.pub_key)
    if peers_to_create:
        Peers.objects.bulk_create(peers_to_create)
    if peers_to_update:
        Peers.objects.bulk_update(peers_to_update, ['address', 'sat_sent', 'sat_recv', 'inbound', 'ping_time', 'alias', 'connected'])
    Peers.objects.filter(connected=True).exclude(pubkey__in=peer_list).update(connected=False)

def refresh_peer_aliases(stub):
    """Refresh aliases for peers without an alias."""
    peers = Peers.objects.filter(Q(alias__isnull=True) | Q(alias=''))
    for peer in peers:
        try:
            alias = get_node_info_cached(peer.pubkey, stub).node.alias
        except Exception:
            alias = None
        if alias:
            peer.alias = alias
            peer.save()

def update_onchain(stub):
    Onchain.objects.filter(block_height=0).delete()
    last_block = 0 if Onchain.objects.aggregate(Max('block_height'))['block_height__max'] == None else Onchain.objects.aggregate(Max('block_height'))['block_height__max'] + 1
    onchain_txs = stub.GetTransactions(ln.GetTransactionsRequest(start_height=last_block)).transactions
    for tx in onchain_txs:
        Onchain(tx_hash=tx.tx_hash, time_stamp=datetime.fromtimestamp(tx.time_stamp), amount=tx.amount, fee=tx.total_fees, block_hash=tx.block_hash, block_height=tx.block_height, label=tx.label[:100]).save()

def network_links():
    if LocalSettings.objects.filter(key='GUI-NetLinks').exists():
        network_links = str(LocalSettings.objects.filter(key='GUI-NetLinks')[0].value)
    else:
        LocalSettings(key='GUI-NetLinks', value='https://mempool.space').save()
        network_links = 'https://mempool.space'
    return network_links

def get_tx_fees(txid):
    base_url = network_links() + ('/testnet' if settings.LND_NETWORK == 'testnet' else '') + '/api/tx/'
    try:
        request_data = get(base_url + txid).json()
        fee = request_data['fee']
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Data] : Error getting closure fees for {txid}: {str(e)}")
        fee = 0
    return fee

def update_closures(stub):
    closures = stub.ClosedChannels(ln.ClosedChannelsRequest()).channels
    if len(closures) > Closures.objects.all().count():
        counter = 0
        skip = Closures.objects.all().count()
        for closure in closures:
            counter += 1
            if counter > skip:
                resolution_count = len(closure.resolutions)
                txid, index = closure.channel_point.split(':')
                closing_costs = get_tx_fees(closure.closing_tx_hash) if (closure.open_initiator != 2 and closure.close_type not in [4, 5]) else 0
                db_closure = Closures(chan_id=closure.chan_id, funding_txid=txid, funding_index=index, closing_tx=closure.closing_tx_hash, remote_pubkey=closure.remote_pubkey, capacity=closure.capacity, close_height=closure.close_height, settled_balance=closure.settled_balance, time_locked_balance=closure.time_locked_balance, close_type=closure.close_type, open_initiator=closure.open_initiator, close_initiator=closure.close_initiator, resolution_count=resolution_count)
                try:
                    db_closure.save()
                except Exception as e:
                    print(f"{datetime.now().strftime('%c')} : [Data] : Error inserting closure: {str(e)}")
                    Closures.objects.filter(funding_txid=txid,funding_index=index).delete()
                    return
                if resolution_count > 0:
                    Resolutions.objects.filter(chan_id=closure.chan_id).delete()
                    for resolution in closure.resolutions:
                        if resolution.resolution_type != 2 and not Resolutions.objects.filter(sweep_txid=resolution.sweep_txid).exists():
                            closing_costs += get_tx_fees(resolution.sweep_txid)
                        Resolutions(chan_id=closure.chan_id, resolution_type=resolution.resolution_type, outcome=resolution.outcome, outpoint_tx=resolution.outpoint.txid_str, outpoint_index=resolution.outpoint.output_index, amount_sat=resolution.amount_sat, sweep_txid=resolution.sweep_txid).save()
                db_closure.closing_costs = closing_costs
                db_closure.save()

def reconnect_peers(stub):
    inactive_peers = Channels.objects.filter(is_open=True, is_active=False, private=False).values_list('remote_pubkey', flat=True).distinct()
    if len(inactive_peers) > 0:
        peers = Peers.objects.all()
        for inactive_peer in inactive_peers:
            if peers.filter(pubkey=inactive_peer).exists():
                peer = peers.filter(pubkey=inactive_peer)[0]
                if peer.last_reconnected == None or (int((datetime.now() - peer.last_reconnected).total_seconds() / 60) > 2):
                    print(f"{datetime.now().strftime('%c')} : [Data] : Reconnecting peer {peer.alias} {peer.pubkey}, last reconnected at {peer.last_reconnected}")
                    if peer.connected == True:
                        print(f"{datetime.now().strftime('%c')} : [Data] : Inactive channel is still connected to peer, disconnecting peer {peer.alias} {inactive_peer}")
                        disconnectpeer(stub, peer)
                    try:
                        node = get_node_info_cached(inactive_peer, stub).node
                        host = node.addresses[0].addr
                    except Exception as e:
                        print(f"{datetime.now().strftime('%c')} : [Data] : Unable to find node info on graph, using last known value for {peer.alias} {peer.pubkey} at {peer.address}: {str(e)}")
                        host = peer.address
                    print(f"{datetime.now().strftime('%c')} : [Data] : Attempting connection to {peer.alias} {inactive_peer} at {host}")
                    try:
                        #try both the graph value and last know value
                        stub.ConnectPeer(request = ln.ConnectPeerRequest(addr=ln.LightningAddress(pubkey=inactive_peer, host=host), perm=True, timeout=5))
                        if host != peer.address and peer.address[:9] != '127.0.0.1':
                            stub.ConnectPeer(request = ln.ConnectPeerRequest(addr=ln.LightningAddress(pubkey=inactive_peer, host=peer.address), perm=True, timeout=5))
                    except Exception as e:
                        error = str(e)
                        details_index = error.find('details =') + 11
                        debug_error_index = error.find('debug_error_string =') - 3
                        error_msg = error[details_index:debug_error_index]
                        print(f"{datetime.now().strftime('%c')} : [Data] : Error reconnecting {peer.alias} {inactive_peer}: {error_msg}")
                    peer.last_reconnected = datetime.now()
                    peer.save()

def clean_payments(stub):
    if LocalSettings.objects.filter(key='LND-CleanPayments').exists():
        enabled = int(LocalSettings.objects.filter(key='LND-CleanPayments')[0].value)
    else:
        LocalSettings(key='LND-CleanPayments', value='0').save()
        enabled = 0
    if enabled == 1:
        if LocalSettings.objects.filter(key='LND-RetentionDays').exists():
            retention_days = int(LocalSettings.objects.filter(key='LND-RetentionDays')[0].value)
        else:
            LocalSettings(key='LND-RetentionDays', value='30').save()
            retention_days = 30
        time_filter = datetime.now() - timedelta(days=retention_days)
        target_payments = Payments.objects.exclude(status=1).filter(cleaned=False).filter(creation_date__lte=time_filter).order_by('index')[:10]
        for payment in target_payments:
            payment_hash = bytes.fromhex(payment.payment_hash)
            htlcs_only = True if payment.status == 2 else False
            try:
                stub.DeletePayment(ln.DeletePaymentRequest(payment_hash=payment_hash, failed_htlcs_only=htlcs_only))
            except Exception as e:
                error = str(e)
                details_index = error.find('details =') + 11
                debug_error_index = error.find('debug_error_string =') - 3
                error_msg = error[details_index:debug_error_index]
                print(f"{datetime.now().strftime('%c')} : [Data] : Error cleaning payment {payment.payment_hash} at index {payment.index} with payment status {payment.status}: {error_msg}")
            finally:
                payment.cleaned = True
                payment.save()

def auto_fees(stub):
    if LocalSettings.objects.filter(key='AF-Enabled').exists():
        if int(LocalSettings.objects.filter(key='AF-Enabled')[0].value) == 0: #disabled
            return
    else:
        LocalSettings(key='AF-Enabled', value='0').save()
        return
    if LocalSettings.objects.filter(key='AF-InboundFees').exists():
        inbound_enabled = int(LocalSettings.objects.filter(key='AF-InboundFees')[0].value)
    else:
        LocalSettings(key='AF-InboundFees', value='0').save()
        inbound_enabled = False
    try:
        channels = Channels.objects.filter(is_open=True, is_active=True, private=False, auto_fees=True)
        results_df = af.main(channels)
        if not results_df.empty:
            update_df = results_df[results_df['eligible'] == True]
            update_df = update_df[(update_df['adjustment']!=0) | (update_df['inbound_adjustment']!=0)]
            if not update_df.empty:
                for target_channel in update_df.to_dict(orient='records'):
                    channel = Channels.objects.filter(chan_id=target_channel['chan_id'])[0]
                    channel_point = ln.ChannelPoint()
                    channel_point.funding_txid_bytes = bytes.fromhex(channel.funding_txid)
                    channel_point.funding_txid_str = channel.funding_txid
                    channel_point.output_index = channel.output_index
                    version = stub.GetInfo(ln.GetInfoRequest()).version
                    if inbound_enabled and float(version[:4]) >= 0.18:
                        inbound_fee_rate = int(target_channel['new_inbound_rate'])
                        # if we are using a discount, then discount our base fee to mirror outbound
                        if inbound_fee_rate == 0:
                            inbound_base_fee = 0
                        else:
                            inbound_base_fee = -channel.local_base_fee
                        stub.UpdateChannelPolicy(ln.PolicyUpdateRequest(chan_point=channel_point, base_fee_msat=channel.local_base_fee, fee_rate=(target_channel['new_rate']/1000000), time_lock_delta=channel.local_cltv, inbound_fee=ln.InboundFee(base_fee_msat=inbound_base_fee, fee_rate_ppm=inbound_fee_rate)))
                        if target_channel['inbound_adjustment'] != 0:
                            print(f"{datetime.now().strftime('%c')} : [Data] : Updating inbound fees for channel {str(target_channel['chan_id'])} to a value of: {str(target_channel['new_inbound_rate'])}")
                            channel.local_inbound_fee_rate = target_channel['new_inbound_rate']
                            InboundFeeLog(chan_id=channel.chan_id, peer_alias=channel.alias, setting=(f"AF [ {target_channel['net_routed_7day']}:{target_channel['in_percent']}:{target_channel['out_percent']} ]"), old_value=target_channel['local_inbound_fee_rate'], new_value=target_channel['new_inbound_rate']).save()
                    else:
                        stub.UpdateChannelPolicy(ln.PolicyUpdateRequest(chan_point=channel_point, base_fee_msat=channel.local_base_fee, fee_rate=(target_channel['new_rate']/1000000), time_lock_delta=channel.local_cltv))
                    if target_channel['adjustment'] != 0:
                        print(f"{datetime.now().strftime('%c')} : [Data] : Updating outbound fees for channel {str(target_channel['chan_id'])} to a value of: {str(target_channel['new_rate'])}")
                        channel.local_fee_rate = target_channel['new_rate']
                        Autofees(chan_id=channel.chan_id, peer_alias=channel.alias, setting=(f"AF [ {target_channel['net_routed_7day']}:{target_channel['in_percent']}:{target_channel['out_percent']} ]"), old_value=target_channel['local_fee_rate'], new_value=target_channel['new_rate']).save()
                    channel.fees_updated = datetime.now()
                    channel.save()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Data] : Error processing auto_fees: {str(e)}")

def inbound_offsets(stub):
    if LocalSettings.objects.filter(key='IO-Enabled').exists():
        if int(LocalSettings.objects.filter(key='IO-Enabled')[0].value) == 0:
            return
    else:
        LocalSettings(key='IO-Enabled', value='0').save()
        return
    if LocalSettings.objects.filter(key='IO-UpdateHours').exists():
        update_hours = float(LocalSettings.objects.filter(key='IO-UpdateHours')[0].value)
    else:
        LocalSettings(key='IO-UpdateHours', value='24').save()
        update_hours = 24.0
    threshold = datetime.now() - timedelta(hours=update_hours)
    version = stub.GetInfo(ln.GetInfoRequest()).version
    if float(version[:4]) < 0.18:
        return
    channels = Channels.objects.filter(is_open=True).exclude(inbound_offset=0)
    for ch in channels:
        if not ch.offset_updated or ch.offset_updated < threshold:
            balance = ch.local_fee_rate + ch.inbound_offset
            target = -balance if balance > 0 else 0
            channel_point = ln.ChannelPoint()
            channel_point.funding_txid_bytes = bytes.fromhex(ch.funding_txid)
            channel_point.funding_txid_str = ch.funding_txid
            channel_point.output_index = ch.output_index
            inbound_base_fee = ch.local_inbound_base_fee if ch.local_inbound_base_fee else 0
            try:
                stub.UpdateChannelPolicy(
                    ln.PolicyUpdateRequest(
                        chan_point=channel_point,
                        base_fee_msat=ch.local_base_fee,
                        fee_rate=(ch.local_fee_rate/1000000),
                        time_lock_delta=ch.local_cltv,
                        inbound_fee=ln.InboundFee(base_fee_msat=inbound_base_fee, fee_rate_ppm=target)
                    )
                )
            except Exception as e:
                print(f"{datetime.now().strftime('%c')} : [Data] : Error updating inbound offset for {ch.chan_id}: {str(e)}")
                continue
            if ch.local_inbound_fee_rate != target:
                InboundFeeLog(chan_id=ch.chan_id, peer_alias=ch.alias, setting='Offset Job', old_value=ch.local_inbound_fee_rate, new_value=target).save()
            ch.local_inbound_fee_rate = target
            ch.offset_updated = datetime.now()
            ch.save()

def auto_maxhtlc_job(stub):
    if LocalSettings.objects.filter(key='MX-Enabled').exists():
        if int(LocalSettings.objects.filter(key='MX-Enabled')[0].value) == 0:
            return
    else:
        LocalSettings(key='MX-Enabled', value='0').save()
        return
    if LocalSettings.objects.filter(key='MX-UpdateHours').exists():
        update_hours = float(LocalSettings.objects.filter(key='MX-UpdateHours')[0].value)
    else:
        LocalSettings(key='MX-UpdateHours', value='24').save()
        update_hours = 24.0
    if LocalSettings.objects.filter(key='MX-Percent').exists():
        global_percent = int(LocalSettings.objects.filter(key='MX-Percent')[0].value)
    else:
        LocalSettings(key='MX-Percent', value='0').save()
        global_percent = 0
    threshold = datetime.now() - timedelta(hours=update_hours)
    channels = Channels.objects.filter(is_open=True)
    for ch in channels:
        outbound = ch.local_balance + ch.pending_outbound
        expected_msat = None
        if ch.mx_liq_upper and outbound < ch.mx_liq_upper:
            expected_msat = ch.mx_liq_value * 1000
        elif ch.mx_liq_threshold and outbound < ch.mx_liq_threshold:
            expected_msat = ch.mx_liq_value * 1000
        else:
            percent = ch.maxhtlc_percent if ch.maxhtlc_percent else global_percent
            if percent:
                expected_msat = int(outbound * (100 - percent) / 100) * 1000
        if expected_msat is None:
            continue
        if (ch.local_max_htlc_msat != expected_msat) or not ch.maxhtlc_updated or ch.maxhtlc_updated < threshold:
            channel_point = ln.ChannelPoint()
            channel_point.funding_txid_bytes = bytes.fromhex(ch.funding_txid)
            channel_point.funding_txid_str = ch.funding_txid
            channel_point.output_index = ch.output_index
            try:
                stub.UpdateChannelPolicy(
                    ln.PolicyUpdateRequest(
                        chan_point=channel_point,
                        base_fee_msat=ch.local_base_fee,
                        fee_rate=(ch.local_fee_rate/1000000),
                        time_lock_delta=ch.local_cltv,
                        max_htlc_msat=expected_msat,
                    )
                )
            except Exception as e:
                print(f"{datetime.now().strftime('%c')} : [Data] : Error updating max htlc for {ch.chan_id}: {str(e)}")
                continue
            ch.local_max_htlc_msat = expected_msat
            ch.maxhtlc_updated = datetime.now()
            ch.save()

def emergency_fee_job(stub):
    if LocalSettings.objects.filter(key='EP-Enabled').exists():
        if int(LocalSettings.objects.filter(key='EP-Enabled')[0].value) == 0:
            return
    else:
        LocalSettings(key='EP-Enabled', value='0').save()
        return
    default_target = int(LocalSettings.objects.filter(key='EP-DefaultTarget').first().value)
    default_inc = float(LocalSettings.objects.filter(key='EP-IncreasePct').first().value)
    default_cooldown = int(LocalSettings.objects.filter(key='EP-Cooldown').first().value)
    channels = Channels.objects.filter(is_open=True, ep_enabled=True)
    for ch in channels:
        target = ch.ep_target if ch.ep_target is not None else default_target
        inc_pct = ch.ep_inc_pct if ch.ep_inc_pct is not None else default_inc
        cooldown = ch.ep_cooldown if ch.ep_cooldown is not None else default_cooldown
        percent = (ch.local_balance + ch.pending_outbound) * 100 / ch.capacity if ch.capacity else 0
        if percent < target:
            if not ch.ep_updated or (datetime.now() - ch.ep_updated).total_seconds() >= cooldown * 60:
                new_rate = int(ch.local_fee_rate * (1 + inc_pct/100))
                channel_point = ln.ChannelPoint()
                channel_point.funding_txid_bytes = bytes.fromhex(ch.funding_txid)
                channel_point.funding_txid_str = ch.funding_txid
                channel_point.output_index = ch.output_index
                try:
                    stub.UpdateChannelPolicy(ln.PolicyUpdateRequest(
                        chan_point=channel_point,
                        base_fee_msat=ch.local_base_fee,
                        fee_rate=(new_rate/1000000),
                        time_lock_delta=ch.local_cltv))
                    Autofees(chan_id=ch.chan_id, peer_alias=ch.alias, setting='EP', old_value=ch.local_fee_rate, new_value=new_rate).save()
                    ch.local_fee_rate = new_rate
                    ch.fees_updated = datetime.now()
                    ch.ep_updated = datetime.now()
                    ch.save()
                except Exception as e:
                    print(f"{datetime.now().strftime('%c')} : [Data] : Error updating emergency fee for {ch.chan_id}: {str(e)}")


def failed_htlc_boost_job(stub):
    """
    Independent failed HTLC boost mechanism.
    Checks EVERY AF-HTLCBoostIntvl minutes if failed HTLCs >= threshold, then applies boost.
    """
    # Get settings with defaults
    try:
        boost_interval = int(LocalSettings.objects.filter(key='AF-HTLCBoostIntvl').first().value) if LocalSettings.objects.filter(key='AF-HTLCBoostIntvl').exists() else 15
    except:
        boost_interval = 15

    try:
        boost_threshold = int(LocalSettings.objects.filter(key='AF-FailedHTLCs').first().value) if LocalSettings.objects.filter(key='AF-FailedHTLCs').exists() else 5
    except:
        boost_threshold = 5

    try:
        boost_amount = int(LocalSettings.objects.filter(key='AF-FailedHTLCBoost').first().value) if LocalSettings.objects.filter(key='AF-FailedHTLCBoost').exists() else 0
    except:
        boost_amount = 0

    try:
        lowliq_limit = int(LocalSettings.objects.filter(key='AF-LowLiqLimit').first().value) if LocalSettings.objects.filter(key='AF-LowLiqLimit').exists() else 5
    except:
        lowliq_limit = 5

    # If boost disabled, skip
    if boost_amount <= 0:
        return

    # Get channels below liquidity limit
    channels = Channels.objects.filter(is_open=True)

    for ch in channels:
        # Calculate liquidity percentage
        local_percent = (ch.local_balance + ch.pending_outbound) * 100 / ch.capacity if ch.capacity else 0

        # Only process channels below liquidity limit
        if local_percent > lowliq_limit:
            continue

        # Check if we should run the check: only every AF-HTLCBoostIntvl minutes
        # Use htlc_boost_checked field to track last check time (or create one if needed)
        threshold_time = datetime.now() - timedelta(minutes=boost_interval)

        # Check if enough time has passed since last boost check
        if ch.htlc_boost_checked and ch.htlc_boost_checked > threshold_time:
            # Not enough time has passed, skip
            continue

        # Enough time has passed, now check failed HTLCs in the interval window
        filter_boost_interval = datetime.now() - timedelta(minutes=boost_interval)

        # Count failed HTLCs in the interval for this channel's outbound
        failed_htlc_count = FailedHTLCs.objects.filter(
            chan_id_out=ch.chan_id,
            timestamp__gte=filter_boost_interval,
            wire_failure=15,
            failure_detail=6
        ).filter(
            Q(amount__gt=0)
        ).count()

        # Mark that we've checked this channel
        ch.htlc_boost_checked = datetime.now()
        ch.save()

        # If threshold met, apply boost
        if failed_htlc_count >= boost_threshold:
            new_rate = ch.local_fee_rate + boost_amount
            channel_point = ln.ChannelPoint()
            channel_point.funding_txid_bytes = bytes.fromhex(ch.funding_txid)
            channel_point.funding_txid_str = ch.funding_txid
            channel_point.output_index = ch.output_index
            inbound_base_fee = ch.local_inbound_base_fee if ch.local_inbound_base_fee else 0

            try:
                stub.UpdateChannelPolicy(
                    ln.PolicyUpdateRequest(
                        chan_point=channel_point,
                        base_fee_msat=ch.local_base_fee,
                        fee_rate=(new_rate / 1_000_000),
                        time_lock_delta=ch.local_cltv,
                        inbound_fee=ln.InboundFee(
                            base_fee_msat=inbound_base_fee,
                            fee_rate_ppm=ch.local_inbound_fee_rate if ch.local_inbound_fee_rate else 0
                        )
                    )
                )
                print(f"{datetime.now().strftime('%c')} : [Data] : Applied HTLC boost to {ch.chan_id}: {failed_htlc_count} HTLCs >= {boost_threshold} threshold, +{boost_amount} ppm")

                # Log the change
                Autofees(chan_id=ch.chan_id, peer_alias=ch.alias, setting='HTLC Boost Job', old_value=ch.local_fee_rate, new_value=new_rate).save()

                # Update channel
                ch.local_fee_rate = new_rate
                ch.fees_updated = datetime.now()
                ch.save()
            except Exception as e:
                print(f"{datetime.now().strftime('%c')} : [Data] : Error applying HTLC boost to {ch.chan_id}: {str(e)}")


def agg_htlcs(target_htlcs, category):
    try:
        target_ids = target_htlcs.values_list('id')
        agg_htlcs = FailedHTLCs.objects.filter(id__in=target_ids).annotate(day=TruncDay('timestamp')).values('day', 'chan_id_in', 'chan_id_out').annotate(amount=Sum('amount'), fee=Sum('missed_fee'), liq=Avg('chan_out_liq'), pending=Avg('chan_out_pending'), count=Count('id'), chan_in_alias=Max('chan_in_alias'), chan_out_alias=Max('chan_out_alias'))
        for htlc in agg_htlcs:
            if HistFailedHTLC.objects.filter(date=htlc['day'],chan_id_in=htlc['chan_id_in'],chan_id_out=htlc['chan_id_out']).exists():
                htlc_itm = HistFailedHTLC.objects.filter(date=htlc['day'],chan_id_in=htlc['chan_id_in'],chan_id_out=htlc['chan_id_out']).get()
            else:
                htlc_itm = HistFailedHTLC(htlc_count=0, amount_sum=0, fee_sum=0, liq_avg=0, pending_avg=0, balance_count=0, downstream_count=0, other_count=0)
                htlc_itm.date = htlc['day']
                htlc_itm.chan_id_in = htlc['chan_id_in']
                htlc_itm.chan_id_out = htlc['chan_id_out']
                htlc_itm.chan_in_alias = htlc['chan_in_alias']
                htlc_itm.chan_out_alias = htlc['chan_out_alias']
            htlc_itm.htlc_count += htlc['count']
            htlc_itm.amount_sum += htlc['amount']
            htlc_itm.fee_sum += htlc['fee']
            htlc_itm.liq_avg += (htlc['count']/htlc_itm.htlc_count)*((0 if htlc['liq'] is None else htlc['liq'])-htlc_itm.liq_avg)
            htlc_itm.pending_avg += (htlc['count']/htlc_itm.htlc_count)*((0 if htlc['pending'] is None else htlc['pending'])-htlc_itm.pending_avg)
            if category == 'balance':
                htlc_itm.balance_count += htlc['count']
            elif category == 'downstream':
                htlc_itm.downstream_count += htlc['count']
            elif category == 'other':
                htlc_itm.other_count += htlc['count']
            htlc_itm.save()
            FailedHTLCs.objects.filter(id__in=target_ids, chan_id_in=htlc['chan_id_in'], chan_id_out=htlc['chan_id_out']).annotate(day=TruncDay('timestamp')).filter(day=htlc['day']).delete()
    except Exception as e:
        print(f"{datetime.now().strftime('%c')} : [Data] : Error processing agg_htlcs: {str(e)}")

def agg_failed_htlcs():
    time_filter = datetime.now() - timedelta(days=30)
    agg_htlcs(FailedHTLCs.objects.filter(timestamp__lte=time_filter, failure_detail=6)[:100], 'balance')
    agg_htlcs(FailedHTLCs.objects.filter(timestamp__lte=time_filter, failure_detail=99)[:100], 'downstream')
    agg_htlcs(FailedHTLCs.objects.filter(timestamp__lte=time_filter).exclude(failure_detail__in=[6, 99])[:100], 'other')

def probe_routes_job(stub):
    if LocalSettings.objects.filter(key='QR-Enabled').exists():
        if int(LocalSettings.objects.filter(key='QR-Enabled')[0].value) == 0:
            return
    else:
        LocalSettings(key='QR-Enabled', value='0').save()
        return
    if LocalSettings.objects.filter(key='QR-UpdateHours').exists():
        update_hours = float(LocalSettings.objects.filter(key='QR-UpdateHours')[0].value)
    else:
        LocalSettings(key='QR-UpdateHours', value='6').save()
        update_hours = 6.0
    if LocalSettings.objects.filter(key='QR-Amount').exists():
        probe_amount = int(LocalSettings.objects.filter(key='QR-Amount')[0].value)
    else:
        LocalSettings(key='QR-Amount', value='50000').save()
        probe_amount = 50000
    if LocalSettings.objects.filter(key='QR-MaxPerTarget').exists():
        max_per_target = int(LocalSettings.objects.filter(key='QR-MaxPerTarget')[0].value)
    else:
        LocalSettings(key='QR-MaxPerTarget', value='5').save()
        max_per_target = 5
    # Check if enough time has passed since last probe
    last_probe = datetime.min
    if LocalSettings.objects.filter(key='QR-LastProbe').exists():
        try:
            last_probe = datetime.fromisoformat(LocalSettings.objects.filter(key='QR-LastProbe')[0].value)
        except Exception:
            pass
    if datetime.now() - last_probe < timedelta(hours=update_hours):
        return
    # Find auto-rebalance targets that need inbound
    targets = Channels.objects.filter(is_open=True, auto_rebalance=True)
    # Find outbound candidates (channels with enough liquidity to be sources)
    outbound_cans = list(
        Channels.objects.filter(is_open=True)
        .exclude(auto_rebalance=True, ar_source=False)
        .values_list('chan_id', flat=True)
    )
    probed_pubkeys = set()
    total_new = 0
    for ch in targets:
        if ch.remote_pubkey in probed_pubkeys:
            continue
        probed_pubkeys.add(ch.remote_pubkey)
        probed_for_target = 0
        # Get outbound channels excluding channels to same peer
        out_chans = [c for c in outbound_cans if c not in
                     set(Channels.objects.filter(remote_pubkey=ch.remote_pubkey).values_list('chan_id', flat=True))]
        for out_chan in out_chans[:max_per_target]:
            try:
                response = stub.QueryRoutes(
                    ln.QueryRoutesRequest(
                        pub_key=ch.remote_pubkey,
                        amt=probe_amount,
                        outgoing_chan_id=int(out_chan),
                        use_mission_control=True,
                    )
                )
            except Exception as e:
                continue
            if not response or not response.routes:
                continue
            for route in response.routes:
                if not route.hops:
                    continue
                path = "-".join(h.pub_key for h in route.hops)
                route_out_chan = str(route.hops[0].chan_id)
                route_hex = route.SerializeToString().hex()
                if len(route.hops) >= 2:
                    cltv = route.hops[-1].expiry - route.hops[-2].expiry
                else:
                    cltv = 144
                _, created = RebalanceRoute.objects.get_or_create(
                    target_pubkey=ch.remote_pubkey,
                    outgoing_chan_id=route_out_chan,
                    route=path,
                    defaults={"final_cltv_delta": cltv, "route_hex": route_hex},
                )
                if created:
                    total_new += 1
                    probed_for_target += 1
    LocalSettings.objects.update_or_create(key='QR-LastProbe', defaults={'value': datetime.now().isoformat()})
    if total_new:
        print(f"{datetime.now().strftime('%c')} : [Data] : QueryRoutes probe discovered {total_new} new route(s) for {len(probed_pubkeys)} target(s)")

def main():
    channel = get_shared_channel()
    while True:
        print(f"{datetime.now().strftime('%c')} : [Data] : Starting data execution...")
        try:
            stub = lnrpc.LightningStub(channel)
            #Update data
            update_peers(stub)
            refresh_peer_aliases(stub)
            update_channels(stub)
            emergency_fee_job(stub)
            update_invoices(stub)
            update_payments(stub)
            update_forwards(stub)
            update_onchain(stub)
            update_closures(stub)
            reconnect_peers(stub)
            clean_payments(stub)
            auto_fees(stub)
            inbound_offsets(stub)
            auto_maxhtlc_job(stub)
            failed_htlc_boost_job(stub)
            probe_routes_job(stub)
            agg_failed_htlcs()
        except Exception as e:
            print(f"{datetime.now().strftime('%c')} : [Data] : Error processing background data: {str(e)}")
            close_shared_channel()
            channel = get_shared_channel()
        print(f"{datetime.now().strftime('%c')} : [Data] : Data execution completed...sleeping for 20 seconds")
        sleep(20)

if __name__ == '__main__':
    main()
