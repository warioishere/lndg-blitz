from pandas import DataFrame


def mirror_peer_fee_targets(channels_df: DataFrame) -> DataFrame:
    """Mirror the lowest-liquidity fee targets across a peer's channels."""

    if channels_df.empty or 'remote_pubkey' not in channels_df.columns:
        return channels_df

    for _, group in channels_df.groupby('remote_pubkey'):
        if len(group) <= 1:
            continue

        controller_idx = group['out_percent'].idxmin()
        follower_idxs = group.index.difference([controller_idx])
        if follower_idxs.empty:
            continue

        controller_new_rate = channels_df.at[controller_idx, 'new_rate']
        controller_new_inbound_rate = (
            channels_df.at[controller_idx, 'new_inbound_rate']
            if 'new_inbound_rate' in channels_df.columns
            else None
        )
        controller_eligible = (
            bool(channels_df.at[controller_idx, 'eligible'])
            if 'eligible' in channels_df.columns
            else False
        )

        channels_df.loc[follower_idxs, 'new_rate'] = controller_new_rate
        channels_df.loc[follower_idxs, 'adjustment'] = (
            controller_new_rate - channels_df.loc[follower_idxs, 'local_fee_rate']
        )

        if 'new_inbound_rate' in channels_df.columns:
            channels_df.loc[follower_idxs, 'new_inbound_rate'] = (
                controller_new_inbound_rate
            )
            channels_df.loc[follower_idxs, 'inbound_adjustment'] = (
                controller_new_inbound_rate
                - channels_df.loc[follower_idxs, 'local_inbound_fee_rate']
            )

        if 'eligible' in channels_df.columns:
            channels_df.loc[group.index, 'eligible'] = controller_eligible

    return channels_df
