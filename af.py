import django
from django.db.models import Sum
from datetime import datetime, timedelta
from os import environ
from pandas import DataFrame, Series
environ['DJANGO_SETTINGS_MODULE'] = 'lndg.settings'
django.setup()
from gui.models import Forwards, Channels, LocalSettings, FailedHTLCs, Payments
from utils import get_local_setting

def main(channels):
    channels_df = DataFrame.from_records(channels.values())
    if channels_df.shape[0] == 0:
        return DataFrame()
    lookback = get_local_setting('FLP-Lookback', 10, int)
    flp_enabled_global = get_local_setting('FLP-Enabled', '0', str) == '1'
    flp_safety_global = get_local_setting('FLP-Safety', 0, int)
    avg_costs = {}
    for ch in channels:
        payments = (
            Payments.objects.filter(status=2, rebal_chan=ch.chan_id)
            .order_by('-creation_date')[:lookback]
        )
        ppm_values = [
            (p.fee * 1000000 / p.value) for p in payments if p.value
        ]
        if ppm_values:
            avg_costs[ch.chan_id] = int(sum(ppm_values) / len(ppm_values))
        else:
            avg_costs[ch.chan_id] = None
    channels_df['avg_rebalance_cost'] = channels_df['chan_id'].map(avg_costs)
    filter_1day = datetime.now() - timedelta(days=1)
    filter_4h = datetime.now() - timedelta(hours=4)
    filter_7day = datetime.now() - timedelta(days=7)
    max_rate = get_local_setting('AF-MaxRate', 2500, int)
    min_rate = get_local_setting('AF-MinRate', 0, int)
    increment = get_local_setting('AF-Increment', 5, int)
    multiplier = get_local_setting('AF-Multiplier', 5, int)
    failed_htlc_limit = get_local_setting('AF-FailedHTLCs', 25, int)
    update_hours = get_local_setting('AF-UpdateHours', 24.0, float)
    lowliq_limit = get_local_setting('AF-LowLiqLimit', 5, int)
    excess_limit = get_local_setting('AF-ExcessLimit', 95, int)
    lowliq_boost = get_local_setting('AF-LowLiqBoost', 1.0, float)
    boost_ar_only = get_local_setting('AF-LowLiqBoostAR', '0', str) == '1'
    excess_boost = get_local_setting('AF-ExcessBoost', 1.0, float)
    excess_boost_enabled = get_local_setting('AF-ExcessBoostOn', '0', str) == '1'
    peer_rate_check = get_local_setting('AF-PeerRateCheck', '0', str) == '1'
    peer_rate_limit = get_local_setting('AF-PeerRateLimit', 0, int)
    flow_scale = get_local_setting('AF-FlowScale', 1.0, float)
    max_step = get_local_setting('AF-MaxStep', 100, int)
    MAX_NET_FLOW = 3

    def clamp_flow(val):
        if val > MAX_NET_FLOW:
            return MAX_NET_FLOW
        if val < -MAX_NET_FLOW:
            return -MAX_NET_FLOW
        return val
    if lowliq_limit >= excess_limit:
        print('Invalid thresholds detected, using defaults...')
        lowliq_limit = 5
        excess_limit = 95

    # Fetch forwarding data
    forwards = Forwards.objects.filter(forward_date__gte=filter_7day, amt_out_msat__gte=1000000)
    forwards_1d = forwards.filter(forward_date__gte=filter_1day)
    forwards_4h = forwards.filter(forward_date__gte=filter_4h)
    
    forwards_df_in_1d_sum = DataFrame.from_records(
        forwards_1d.values('chan_id_in').annotate(amt_out_msat=Sum('amt_out_msat'), fee=Sum('fee')), 
        index='chan_id_in'
    ) if forwards_1d.exists() else DataFrame()
    
    forwards_df_in_4h_sum = DataFrame.from_records(
        forwards_4h.values('chan_id_in').annotate(amt_out_msat=Sum('amt_out_msat')),
        index='chan_id_in'
    ) if forwards_4h.exists() else DataFrame()
    
    forwards_df_out_4h_sum = DataFrame.from_records(
        forwards_4h.values('chan_id_out').annotate(amt_out_msat=Sum('amt_out_msat')),
        index='chan_id_out'
    ) if forwards_4h.exists() else DataFrame()
    
    forwards_df_in_7d_sum = DataFrame.from_records(
        forwards.values('chan_id_in').annotate(amt_out_msat=Sum('amt_out_msat'), fee=Sum('fee')), 
        index='chan_id_in'
    ) if forwards.exists() else DataFrame()
    
    forwards_df_out_7d_sum = DataFrame.from_records(
        forwards.values('chan_id_out').annotate(amt_out_msat=Sum('amt_out_msat'), fee=Sum('fee')), 
        index='chan_id_out'
    ) if forwards.exists() else DataFrame()

    # Compute per-channel metrics
    if not forwards_df_in_1d_sum.empty:
        channels_df['amt_routed_in_1day'] = channels_df['chan_id'].map(
            forwards_df_in_1d_sum['amt_out_msat'].floordiv(1000)
        ).fillna(0).astype(int)
    else:
        channels_df['amt_routed_in_1day'] = 0
    if not forwards_df_in_7d_sum.empty:
        channels_df['amt_routed_in_7day'] = channels_df['chan_id'].map(
            forwards_df_in_7d_sum['amt_out_msat'].floordiv(1000)
        ).fillna(0).astype(int)
    else:
        channels_df['amt_routed_in_7day'] = 0
    if not forwards_df_out_7d_sum.empty:
        channels_df['amt_routed_out_7day'] = channels_df['chan_id'].map(
            forwards_df_out_7d_sum['amt_out_msat'].floordiv(1000)
        ).fillna(0).astype(int)
    else:
        channels_df['amt_routed_out_7day'] = 0

    channels_df['net_routed_7day'] = (
        (channels_df['amt_routed_out_7day'] - channels_df['amt_routed_in_7day']) / channels_df['capacity']
    ).round(1)
    
    channels_df['local_balance'] = channels_df['local_balance'] + channels_df['pending_outbound']
    channels_df['remote_balance'] = channels_df['remote_balance'] + channels_df['pending_inbound']
    channels_df['out_percent'] = ((channels_df['local_balance'] / channels_df['capacity']) * 100).round(0).astype(int)
    channels_df['in_percent'] = ((channels_df['remote_balance'] / channels_df['capacity']) * 100).round(0).astype(int)
    channels_df['eligible'] = (datetime.now() - channels_df['fees_updated']).dt.total_seconds() > (update_hours * 3600)

    # Compute failed HTLCs per channel
    filter_last_updated = datetime.now() - timedelta(hours=update_hours)
    failed_htlc_df = DataFrame.from_records(
        FailedHTLCs.objects.filter(timestamp__gte=filter_last_updated, wire_failure=15, failure_detail=6).values()
    )
    if not failed_htlc_df.empty:
        failed_htlc_df = failed_htlc_df[
            failed_htlc_df['amount'] > (failed_htlc_df['chan_out_liq'] + failed_htlc_df['chan_out_pending'])
        ]
        failed_out_1day_series = failed_htlc_df['chan_id_out'].value_counts()
    else:
        failed_out_1day_series = Series(dtype='int64')
    channels_df['failed_out_1day'] = channels_df['chan_id'].map(failed_out_1day_series).fillna(0).astype(int)

    # Compute revenue metrics
    if not forwards_df_in_7d_sum.empty:
        channels_df['revenue_assist_7day'] = channels_df['chan_id'].map(
            forwards_df_in_7d_sum['fee']
        ).fillna(0).astype(float)
    else:
        channels_df['revenue_assist_7day'] = 0.0

    if not forwards_df_out_7d_sum.empty:
        channels_df['revenue_7day'] = channels_df['chan_id'].map(
            forwards_df_out_7d_sum['fee']
        ).fillna(0).astype(float)
    else:
        channels_df['revenue_7day'] = 0.0
    if not forwards_df_in_4h_sum.empty:
        channels_df['amt_routed_in_4h'] = channels_df['chan_id'].map(
            forwards_df_in_4h_sum['amt_out_msat'].floordiv(1000)
        ).fillna(0).astype(int)
    else:
        channels_df['amt_routed_in_4h'] = 0
    if not forwards_df_out_4h_sum.empty:
        channels_df['amt_routed_out_4h'] = channels_df['chan_id'].map(
            forwards_df_out_4h_sum['amt_out_msat'].floordiv(1000)
        ).fillna(0).astype(int)
    else:
        channels_df['amt_routed_out_4h'] = 0


    # Aggregate data by remote_pubkey
    group_df = channels_df.groupby('remote_pubkey').agg({
        'local_balance': 'sum',
        'capacity': 'sum',
        'failed_out_1day': 'sum',
        'amt_routed_in_1day': 'sum',
        'amt_routed_in_7day': 'sum',
        'amt_routed_out_7day': 'sum',
        'amt_routed_in_4h': 'sum',      # <---- HINZUGEFÜGT
        'amt_routed_out_4h': 'sum',     # <---- HINZUGEFÜGT
        'revenue_7day': 'sum',
        'revenue_assist_7day': 'sum'
    }).rename(columns={
        'local_balance': 'total_local_balance',
        'capacity': 'total_capacity',
        'failed_out_1day': 'total_failed_out_1day',
        'amt_routed_in_1day': 'total_amt_routed_in_1day',
        'amt_routed_in_7day': 'total_amt_routed_in_7day',
        'amt_routed_out_7day': 'total_amt_routed_out_7day',
        'revenue_7day': 'total_revenue_7day',
        'amt_routed_in_4h': 'total_amt_routed_in_4h',     # <---- HINZUGEFÜGT
        'amt_routed_out_4h': 'total_amt_routed_out_4h',   # <---- HINZUGEFÜGT
        'revenue_assist_7day': 'total_revenue_assist_7day'
    })

    group_df['overall_out_percent'] = (
        (group_df['total_local_balance'] / group_df['total_capacity']) * 100
    ).where(group_df['total_capacity'] > 0, 0)

    group_df['group_net_routed_7day'] = (
        (group_df['total_amt_routed_out_7day'] - group_df['total_amt_routed_in_7day']) / group_df['total_capacity']
    ).where(group_df['total_capacity'] > 0, 0)

    # Define inbound adjustment calculation function
    def clamp_step(val):
        if val > max_step:
            return max_step
        if val < -max_step:
            return -max_step
        return int(val)

    def compute_inbound_adjustment(row):
        if row['overall_out_percent'] <= lowliq_limit:
            adj = (-12 * multiplier) if (
                row['total_failed_out_1day'] > failed_htlc_limit
                and row['total_amt_routed_in_1day'] == 0
            ) else 0
        elif row['overall_out_percent'] < excess_limit:
            if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
                adj = 7 * multiplier
            elif row['group_net_routed_7day'] > 1:
                flow = clamp_flow(row['group_net_routed_7day'])
                scale = 1 + flow * flow_scale
                adj = (-5 * multiplier) * scale
            else:
                adj = 0
        else:
            if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
                adj = 12 * multiplier
            elif (
                row['group_net_routed_7day'] < -1
                and row['total_revenue_assist_7day'] > row['total_revenue_7day'] * 10
            ):
                flow = abs(clamp_flow(row['group_net_routed_7day']))
                scale = 1 + flow * flow_scale
                adj = 12 * multiplier * scale
            else:
                adj = 0
        return clamp_step(adj)

    group_df['inbound_adjustment'] = group_df.apply(compute_inbound_adjustment, axis=1)

    # Merge peer metrics back onto channels_df
    merge_cols = [
        'overall_out_percent', 'group_net_routed_7day',
        'total_failed_out_1day', 'total_amt_routed_in_1day',
        'total_amt_routed_in_7day', 'total_amt_routed_out_7day',
        'total_amt_routed_in_4h', 'total_amt_routed_out_4h',
        'total_revenue_7day', 'total_revenue_assist_7day',
        'inbound_adjustment'
    ]
    channels_df = channels_df.merge(group_df[merge_cols], on='remote_pubkey', how='left')

    # Define outbound adjustment calculation function (per channel)
    def compute_outbound_adjustment(row):
        if row['out_percent'] <= lowliq_limit:
            if peer_rate_check and peer_rate_limit > 0 and row['remote_fee_rate'] >= peer_rate_limit:
                return 0
            boost = 0
            if boost_ar_only and row.get('auto_rebalance'):
                deficit = max(0, lowliq_limit - row['out_percent'])
                boost = deficit / max(lowliq_limit, 1) * lowliq_boost
            return clamp_step(max(1, int(multiplier * boost)))

        if row['overall_out_percent'] <= lowliq_limit:
            adj = (5 * multiplier) if (
                row['total_failed_out_1day'] > failed_htlc_limit
                and row['total_amt_routed_in_1day'] == 0
            ) else 0
            return clamp_step(adj)
        elif row['overall_out_percent'] >= excess_limit:
            adj = -1
            if excess_boost_enabled:
                adj = int(adj * excess_boost)
            return clamp_step(adj)
        elif row['overall_out_percent'] < excess_limit:
            if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
                adj = -3 * multiplier
                if excess_boost_enabled:
                    adj = int(adj * excess_boost)
            elif abs(row['group_net_routed_7day']) > 1:
                flow = clamp_flow(row['group_net_routed_7day'])
                scale = 1 + abs(flow) * flow_scale
                base = 2 * multiplier if flow > 0 else -5 * multiplier
                adj = base * scale
            else:
                adj = 0
            return clamp_step(adj)
        else:
            if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
                adj = -5 * multiplier
                if excess_boost_enabled:
                    adj = int(adj * excess_boost)
            elif (
                row['group_net_routed_7day'] < -1
                and row['total_revenue_assist_7day'] > row['total_revenue_7day'] * 10
            ):
                flow = abs(clamp_flow(row['group_net_routed_7day']))
                scale = 1 + flow * flow_scale
                adj = -5 * multiplier
                if excess_boost_enabled:
                    adj = int(adj * excess_boost)
                adj *= scale
            else:
                adj = 0
            return clamp_step(adj)

    channels_df['adjustment'] = channels_df.apply(compute_outbound_adjustment, axis=1)

    # Compute new outbound rates
    channels_df['new_rate'] = channels_df['local_fee_rate'] + channels_df['adjustment']
    channels_df['new_rate'] = (channels_df['new_rate'] / increment).round(0) * increment
    channels_df['new_rate'] = channels_df['new_rate'].clip(min_rate, max_rate)
    def compute_cost_floor(row):
        if not flp_enabled_global or not row.get('flp_enabled'):
            return 0
        avg_cost = row.get('avg_rebalance_cost')
        if avg_cost is None:
            return 0
        safety = flp_safety_global + row.get('flp_safety', 0)
        return max(avg_cost - safety, 0)

    channels_df['cost_floor'] = (
        channels_df.apply(compute_cost_floor, axis=1)
        .round(0)
        .clip(upper=max_rate)
        .fillna(0)
    )
    channels_df['new_rate'] = channels_df[['new_rate', 'cost_floor']].max(axis=1)
    channels_df['adjustment'] = channels_df['new_rate'] - channels_df['local_fee_rate']

    # Compute new inbound rates
    if 'ar_max_cost' not in channels_df.columns:
        channels_df['ar_max_cost'] = 0

    channels_df['new_inbound_rate'] = channels_df['local_inbound_fee_rate'] + channels_df['inbound_adjustment']
    channels_df['new_inbound_rate'] = (channels_df['new_inbound_rate'] / increment).round(0) * increment
    channels_df['new_inbound_rate'] = channels_df['new_inbound_rate'].clip(-((channels_df['ar_max_cost']/100)*channels_df['local_fee_rate']), 0)
    channels_df['inbound_adjustment'] = channels_df['new_inbound_rate'] - channels_df['local_inbound_fee_rate']

    # Return results
    return channels_df


if __name__ == '__main__':
    print(main(Channels.objects.filter(is_open=True))[['chan_id', 'local_fee_rate', 'new_rate', 'adjustment', 'local_inbound_fee_rate', 'new_inbound_rate', 'inbound_adjustment']])
