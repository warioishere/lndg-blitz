import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tests.django_settings')

dummy_dir = Path(__file__).resolve().parent
(dummy_dir / 'dummy.cert').write_bytes(b'cert')
(dummy_dir / 'dummy.macaroon').write_bytes(b'macaroon')
(dummy_dir / 'dummy.db').write_bytes(b'')

fake_settings = types.ModuleType('lndg.settings')
fake_settings.LND_MACAROON_PATH = str(dummy_dir / 'dummy.macaroon')
fake_settings.LND_TLS_PATH = str(dummy_dir / 'dummy.cert')
fake_settings.LND_RPC_SERVER = 'localhost:10009'
fake_settings.LND_MAX_MESSAGE = '8'
fake_settings.LND_NETWORK = 'mainnet'
fake_settings.LOGIN_REQUIRED = False
fake_settings.LND_DATABASE_PATH = str(dummy_dir / 'dummy.db')
sys.modules['lndg.settings'] = fake_settings

fake_lnd_connect = types.ModuleType('gui.lnd_deps.lnd_connect')
fake_lnd_connect.lnd_connect = lambda: None
fake_lnd_connect.async_lnd_connect = lambda: None
fake_lnd_connect.get_shared_channel = lambda: None
fake_lnd_connect.get_shared_async_channel = lambda: None
fake_lnd_connect.close_shared_channel = lambda: None
fake_lnd_connect.close_shared_async_channel = lambda: None
sys.modules['gui.lnd_deps.lnd_connect'] = fake_lnd_connect

import django

django.setup()

from django.core.management import call_command

call_command('migrate', run_syncdb=True, verbosity=0)

from django.test import TestCase
from django.utils import timezone

from gui.models import Channels, Autofees
from gui.views import sync_peer_outbound_fee


def make_channel(chan_id: str, remote_pubkey: str, alias: str, local_fee_rate: int, *, output_index: int) -> Channels:
    now = timezone.now()
    return Channels.objects.create(
        remote_pubkey=remote_pubkey,
        chan_id=chan_id,
        short_chan_id=chan_id,
        funding_txid=f"{output_index:064x}",
        output_index=output_index,
        capacity=1_000_000,
        local_balance=500_000,
        remote_balance=500_000,
        unsettled_balance=0,
        local_commit=0,
        local_chan_reserve=0,
        num_updates=0,
        initiator=True,
        alias=alias,
        total_sent=0,
        total_received=0,
        private=False,
        pending_outbound=0,
        pending_inbound=0,
        htlc_count=0,
        local_base_fee=0,
        local_fee_rate=local_fee_rate,
        local_inbound_base_fee=0,
        local_inbound_fee_rate=0,
        inbound_offset=0,
        offset_updated=now,
        maxhtlc_percent=0,
        maxhtlc_updated=now,
        mx_liq_threshold=0,
        mx_liq_value=0,
        mx_liq_upper=0,
        local_disabled=False,
        local_cltv=40,
        local_min_htlc_msat=1000,
        local_max_htlc_msat=1_000_000_000,
        remote_base_fee=0,
        remote_fee_rate=0,
        remote_inbound_base_fee=0,
        remote_inbound_fee_rate=0,
        remote_disabled=False,
        remote_cltv=40,
        remote_min_htlc_msat=1000,
        remote_max_htlc_msat=1_000_000_000,
        push_amt=0,
        close_address='',
        is_active=True,
        is_open=True,
        last_update=now,
        auto_rebalance=False,
        ar_amt_target=0,
        ar_in_target=0,
        ar_out_target=0,
        ar_max_cost=0,
        ar_source=False,
        ar_source_ppm_diff=0,
        ep_enabled=False,
        ep_target=50,
        ep_inc_pct=10,
        ep_cooldown=10,
        ep_live_threshold=40,
        ep_live_inc_pct=5,
        flp_enabled=False,
        flp_safety=0,
        ep_updated=now,
        fees_updated=now,
        auto_fees=False,
        notes='',
    )


class SyncPeerOutboundFeeTests(TestCase):
    databases = {'default'}

    def test_sync_peer_outbound_fee_updates_siblings(self):
        stub = MagicMock()
        controller = make_channel('1001', 'peer-a', 'PeerA', 500, output_index=1)
        sibling = make_channel('1002', 'peer-a', 'PeerA', 900, output_index=2)
        other = make_channel('1003', 'peer-b', 'PeerB', 700, output_index=3)

        original_updated = sibling.fees_updated

        processed, updated = sync_peer_outbound_fee(controller, 600, stub=stub)

        self.assertIn(sibling.chan_id, processed)
        self.assertIn(sibling.chan_id, updated)
        self.assertNotIn(other.chan_id, processed)

        sibling.refresh_from_db()
        self.assertEqual(sibling.local_fee_rate, 600)
        self.assertNotEqual(sibling.fees_updated, original_updated)
        self.assertEqual(Autofees.objects.filter(chan_id=sibling.chan_id).count(), 1)

        stub.UpdateChannelPolicy.assert_called()
        request = stub.UpdateChannelPolicy.call_args[0][0]
        self.assertAlmostEqual(request.fee_rate, 600 / 1_000_000)

    def test_sync_peer_outbound_fee_skips_matching_rates(self):
        stub = MagicMock()
        controller = make_channel('2001', 'peer-c', 'PeerC', 500, output_index=11)
        sibling = make_channel('2002', 'peer-c', 'PeerC', 600, output_index=12)

        processed, updated = sync_peer_outbound_fee(controller, 600, stub=stub)

        self.assertIn(sibling.chan_id, processed)
        self.assertEqual(len(updated), 0)
        stub.UpdateChannelPolicy.assert_not_called()
        self.assertEqual(Autofees.objects.filter(chan_id=sibling.chan_id).count(), 0)
