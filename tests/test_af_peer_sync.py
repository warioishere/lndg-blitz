from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from peer_fee_sync import mirror_peer_fee_targets


def test_mirror_peer_fee_targets_syncs_sibling_channels():
    channels_df = pd.DataFrame([
        {
            'chan_id': 111,
            'remote_pubkey': 'peer-1',
            'out_percent': 35,
            'local_fee_rate': 700,
            'new_rate': 600,
            'adjustment': -100,
            'local_inbound_fee_rate': 0,
            'new_inbound_rate': -50,
            'inbound_adjustment': -50,
            'eligible': True,
        },
        {
            'chan_id': 222,
            'remote_pubkey': 'peer-1',
            'out_percent': 80,
            'local_fee_rate': 900,
            'new_rate': 950,
            'adjustment': 50,
            'local_inbound_fee_rate': 0,
            'new_inbound_rate': 0,
            'inbound_adjustment': 0,
            'eligible': False,
        },
        {
            'chan_id': 333,
            'remote_pubkey': 'peer-2',
            'out_percent': 40,
            'local_fee_rate': 500,
            'new_rate': 520,
            'adjustment': 20,
            'local_inbound_fee_rate': 0,
            'new_inbound_rate': 0,
            'inbound_adjustment': 0,
            'eligible': True,
        },
    ])

    mirrored_df = mirror_peer_fee_targets(channels_df.copy())

    follower = mirrored_df[mirrored_df['chan_id'] == 222].iloc[0]
    controller = mirrored_df[mirrored_df['chan_id'] == 111].iloc[0]
    independent = mirrored_df[mirrored_df['chan_id'] == 333].iloc[0]

    assert follower['new_rate'] == controller['new_rate']
    assert follower['adjustment'] == controller['new_rate'] - 900
    assert follower['new_inbound_rate'] == controller['new_inbound_rate']
    assert follower['inbound_adjustment'] == controller['new_inbound_rate'] - 0
    assert follower['eligible'] == controller['eligible']

    # Ensure unrelated peers remain unchanged
    assert independent['new_rate'] == 520
    assert independent['adjustment'] == 20
