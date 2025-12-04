from django.db import models
from django.utils import timezone
from django.db.models import (
    ExpressionWrapper,
    Value,
    FloatField,
)
from django.db.models.functions import Cast


def _is_django_expression(value) -> bool:
    """Return True if value behaves like a Django expression (e.g., F)."""
    return hasattr(value, "resolve_expression")

# Create your models here.
class Payments(models.Model):
    creation_date = models.DateTimeField()
    payment_hash = models.CharField(max_length=64, primary_key=True)
    value = models.FloatField()
    fee = models.FloatField()
    status = models.IntegerField()
    index = models.IntegerField()
    chan_out = models.CharField(max_length=20, null=True)
    chan_out_alias = models.CharField(null=True, max_length=32)
    keysend_preimage = models.CharField(null=True, max_length=64)
    message = models.CharField(null=True, max_length=1000)
    cleaned = models.BooleanField(default=False)
    rebal_chan = models.CharField(max_length=20, null=True)
    class Meta:
        app_label = 'gui'
        indexes = [
            models.Index(fields=['index'], name='payments_index_idx'),
            models.Index(fields=['chan_out'], name='payments_chan_out_idx'),
            models.Index(fields=['rebal_chan'], name='payments_rebal_chan_idx'),
        ]

class PaymentHops(models.Model):
    payment_hash = models.ForeignKey('Payments', on_delete=models.CASCADE)
    attempt_id = models.IntegerField()
    step = models.IntegerField()
    chan_id = models.CharField(max_length=20)
    alias = models.CharField(max_length=32)
    chan_capacity = models.BigIntegerField()
    node_pubkey = models.CharField(max_length=66)
    amt = models.FloatField()
    fee = models.FloatField()
    cost_to = models.FloatField()
    class Meta:
        app_label = 'gui'
        unique_together = (('payment_hash', 'attempt_id', 'step'),)
        indexes = [
            models.Index(fields=['chan_id'], name='paymenthops_chanid_idx'),
            models.Index(fields=['node_pubkey'], name='paymenthops_node_pubkey_idx'),
        ]

class Invoices(models.Model):
    creation_date = models.DateTimeField()
    settle_date = models.DateTimeField(null=True, default=None)
    r_hash = models.CharField(max_length=64, primary_key=True)
    value = models.FloatField()
    amt_paid = models.BigIntegerField()
    state = models.IntegerField()
    chan_in = models.CharField(max_length=20, null=True)
    chan_in_alias = models.CharField(null=True, max_length=32)
    keysend_preimage = models.CharField(null=True, max_length=64)
    message = models.CharField(null=True, max_length=1000)
    sender = models.CharField(null=True, max_length=66)
    sender_alias = models.CharField(null=True, max_length=32)
    index = models.IntegerField()
    is_revenue = models.BooleanField(default=False)
    class Meta:
        app_label = 'gui'

class Forwards(models.Model):
    forward_date = models.DateTimeField()
    chan_id_in = models.CharField(max_length=20)
    chan_id_out = models.CharField(max_length=20)
    chan_in_alias = models.CharField(null=True, max_length=32)
    chan_out_alias = models.CharField(null=True, max_length=32)
    amt_in_msat = models.BigIntegerField()
    amt_out_msat = models.BigIntegerField()
    fee = models.FloatField()
    inbound_fee = models.FloatField()
    class Meta:
        app_label = 'gui'
        indexes = [
            models.Index(fields=['forward_date'], name='forwards_date_idx'),
            models.Index(fields=['chan_id_in'], name='forwards_in_idx'),
            models.Index(fields=['chan_id_out'], name='forwards_out_idx'),
            models.Index(fields=['chan_id_in', 'forward_date'], name='forwards_in_date_idx'),
            models.Index(fields=['chan_id_out', 'forward_date'], name='forwards_out_date_idx'),
        ]

class Channels(models.Model):
    remote_pubkey = models.CharField(max_length=66)
    chan_id = models.CharField(max_length=20, primary_key=True)
    short_chan_id = models.CharField(max_length=20)
    funding_txid = models.CharField(max_length=64)
    output_index = models.IntegerField()
    capacity = models.BigIntegerField()
    local_balance = models.BigIntegerField()
    remote_balance = models.BigIntegerField()
    unsettled_balance = models.BigIntegerField()
    local_commit = models.IntegerField()
    local_chan_reserve = models.IntegerField()
    num_updates = models.IntegerField()
    initiator = models.BooleanField()
    alias = models.CharField(max_length=32)
    total_sent = models.BigIntegerField()
    total_received = models.BigIntegerField()
    private = models.BooleanField()
    pending_outbound = models.BigIntegerField()
    pending_inbound = models.BigIntegerField()
    htlc_count = models.IntegerField()
    local_base_fee = models.IntegerField()
    local_fee_rate = models.IntegerField()
    local_inbound_base_fee = models.IntegerField()
    local_inbound_fee_rate = models.IntegerField()
    inbound_offset = models.IntegerField(default=0)
    offset_updated = models.DateTimeField(null=True, default=None)
    maxhtlc_percent = models.IntegerField(default=0)
    maxhtlc_updated = models.DateTimeField(null=True, default=None)
    mx_liq_threshold = models.BigIntegerField(default=0)
    mx_liq_value = models.BigIntegerField(default=0)
    mx_liq_upper = models.BigIntegerField(default=0)
    local_disabled = models.BooleanField()
    local_cltv = models.IntegerField()
    local_min_htlc_msat = models.BigIntegerField()
    local_max_htlc_msat = models.BigIntegerField()
    remote_base_fee = models.IntegerField()
    remote_fee_rate = models.IntegerField()
    remote_inbound_base_fee = models.IntegerField()
    remote_inbound_fee_rate = models.IntegerField()
    remote_disabled = models.BooleanField()
    remote_cltv = models.IntegerField()
    remote_min_htlc_msat = models.BigIntegerField()
    remote_max_htlc_msat = models.BigIntegerField()
    push_amt = models.BigIntegerField()
    close_address = models.CharField(max_length=100)
    is_active = models.BooleanField()
    is_open = models.BooleanField()
    last_update = models.DateTimeField()
    auto_rebalance = models.BooleanField(default=False)
    ar_amt_target = models.BigIntegerField()
    ar_in_target = models.IntegerField()
    ar_out_target = models.IntegerField()
    ar_max_cost = models.IntegerField()
    ar_source = models.BooleanField(default=False)
    ar_source_ppm_diff = models.IntegerField(default=0)
    ep_enabled = models.BooleanField(default=False)
    ep_target = models.IntegerField(default=50)
    ep_inc_pct = models.FloatField(default=10)
    ep_cooldown = models.IntegerField(default=10)
    ep_live_threshold = models.IntegerField(default=40)
    ep_live_inc_pct = models.FloatField(default=5)
    flp_enabled = models.BooleanField(default=False)
    flp_safety = models.IntegerField(default=0)
    ep_updated = models.DateTimeField(default=timezone.now)
    fees_updated = models.DateTimeField(default=timezone.now)
    htlc_boost_checked = models.DateTimeField(null=True, default=None)
    auto_fees = models.BooleanField()
    notes = models.TextField(default='', blank=True)

    def save(self, *args, **kwargs):
        if self.auto_fees is None:
            if LocalSettings.objects.filter(key='AF-Enabled').exists():
                enabled = int(LocalSettings.objects.filter(key='AF-Enabled')[0].value)
            else:
                LocalSettings(key='AF-Enabled', value='0').save()
                enabled = 0
            self.auto_fees = False if enabled == 0 else True
        if not self.ar_out_target:
            if LocalSettings.objects.filter(key='AR-Outbound%').exists():
                outbound_setting = int(LocalSettings.objects.filter(key='AR-Outbound%')[0].value)
            else:
                LocalSettings(key='AR-Outbound%', value='75').save()
                outbound_setting = 75
            self.ar_out_target = outbound_setting
        if not self.ar_in_target:
            if LocalSettings.objects.filter(key='AR-Inbound%').exists():
                inbound_setting = int(LocalSettings.objects.filter(key='AR-Inbound%')[0].value)
            else:
                LocalSettings(key='AR-Inbound%', value='90').save()
                inbound_setting = 90
            self.ar_in_target = inbound_setting
        if not self.ar_amt_target:
            if LocalSettings.objects.filter(key='AR-Target%').exists():
                amt_setting = float(LocalSettings.objects.filter(key='AR-Target%')[0].value)
            else:
                LocalSettings(key='AR-Target%', value='3').save()
                amt_setting = 3
            self.ar_amt_target = int((amt_setting/100) * self.capacity)
        if not self.ar_max_cost:
            if LocalSettings.objects.filter(key='AR-MaxCost%').exists():
                cost_setting = int(LocalSettings.objects.filter(key='AR-MaxCost%')[0].value)
            else:
                LocalSettings(key='AR-MaxCost%', value='65').save()
                cost_setting = 65
            self.ar_max_cost = cost_setting
        if not self.ep_target:
            if LocalSettings.objects.filter(key='EP-DefaultTarget').exists():
                ep_setting = int(LocalSettings.objects.filter(key='EP-DefaultTarget')[0].value)
            else:
                LocalSettings(key='EP-DefaultTarget', value='50').save()
                ep_setting = 50
            self.ep_target = ep_setting
        if self.ep_enabled is None:
            if LocalSettings.objects.filter(key='EP-Enabled').exists():
                enabled = int(LocalSettings.objects.filter(key='EP-Enabled')[0].value)
            else:
                LocalSettings(key='EP-Enabled', value='0').save()
                enabled = 0
            self.ep_enabled = False if enabled == 0 else True
        if not self.ep_inc_pct:
            if LocalSettings.objects.filter(key='EP-IncreasePct').exists():
                inc_pct = float(LocalSettings.objects.filter(key='EP-IncreasePct')[0].value)
            else:
                LocalSettings(key='EP-IncreasePct', value='10').save()
                inc_pct = 10
            self.ep_inc_pct = inc_pct
        if not self.ep_cooldown:
            if LocalSettings.objects.filter(key='EP-Cooldown').exists():
                cooldown = int(LocalSettings.objects.filter(key='EP-Cooldown')[0].value)
            else:
                LocalSettings(key='EP-Cooldown', value='10').save()
                cooldown = 10
            self.ep_cooldown = cooldown
        if not self.ep_live_threshold:
            if LocalSettings.objects.filter(key='EP-LiveThreshold').exists():
                threshold = int(LocalSettings.objects.filter(key='EP-LiveThreshold')[0].value)
            else:
                LocalSettings(key='EP-LiveThreshold', value='40').save()
                threshold = 40
            self.ep_live_threshold = threshold
        if not self.ep_live_inc_pct:
            if LocalSettings.objects.filter(key='EP-LiveIncreasePct').exists():
                live_inc = float(LocalSettings.objects.filter(key='EP-LiveIncreasePct')[0].value)
            else:
                LocalSettings(key='EP-LiveIncreasePct', value='5').save()
                live_inc = 5
            self.ep_live_inc_pct = live_inc
        super(Channels, self).save(*args, **kwargs)

    class Meta:
        app_label = 'gui'


class AllowedTarget(models.Model):
    source_chan = models.ForeignKey(Channels, on_delete=models.CASCADE)
    target_pubkey = models.CharField(max_length=66)

    class Meta:
        app_label = 'gui'
        unique_together = (('source_chan', 'target_pubkey'),)

class Peers(models.Model):
    pubkey = models.CharField(max_length=66, primary_key=True)
    alias = models.CharField(null=True, max_length=32)
    address = models.CharField(max_length=100)
    sat_sent = models.BigIntegerField()
    sat_recv = models.BigIntegerField()
    inbound = models.BooleanField()
    connected = models.BooleanField()
    last_reconnected = models.DateTimeField(null=True, default=None)
    ping_time = models.BigIntegerField(default=0)
    class Meta:
        app_label = 'gui'

class Rebalancer(models.Model):
    requested = models.DateTimeField(default=timezone.now)
    value = models.IntegerField()
    fee_limit = models.FloatField()
    outgoing_chan_ids = models.TextField(default='[]')
    last_hop_pubkey = models.CharField(default='', max_length=66)
    target_alias = models.CharField(default='', max_length=32)
    duration = models.IntegerField()
    start = models.DateTimeField(null=True, default=None)
    stop = models.DateTimeField(null=True, default=None)
    status = models.IntegerField(default=0)
    payment_hash = models.CharField(max_length=64, null=True, default=None)
    manual = models.BooleanField(default=False)
    fees_paid = models.FloatField(null=True, default=None)
    class Meta:
        app_label = 'gui'

class LocalSettings(models.Model):
    key = models.CharField(primary_key=True, default=None, max_length=20)
    # allow arbitrarily long values for settings like the Amboss API key
    value = models.TextField(default=None)
    class Meta:
        app_label = 'gui'

class Onchain(models.Model):
    tx_hash = models.CharField(max_length=64, primary_key=True)
    amount = models.BigIntegerField()
    block_hash = models.CharField(max_length=64)
    block_height = models.IntegerField()
    time_stamp = models.DateTimeField()
    fee = models.IntegerField()
    label = models.CharField(max_length=100)
    class Meta:
        app_label = 'gui'

class Closures(models.Model):
    chan_id = models.CharField(max_length=20)
    funding_txid = models.CharField(max_length=64)
    funding_index = models.IntegerField()
    closing_tx = models.CharField(max_length=64)
    remote_pubkey = models.CharField(max_length=66)
    capacity = models.BigIntegerField()
    close_height = models.IntegerField()
    settled_balance = models.BigIntegerField()
    time_locked_balance = models.BigIntegerField()
    close_type = models.IntegerField()
    open_initiator = models.IntegerField()
    close_initiator = models.IntegerField()
    resolution_count = models.IntegerField()
    closing_costs = models.IntegerField(default=0)
    class Meta:
        app_label = 'gui'
        unique_together = (('funding_txid', 'funding_index'),)

class Resolutions(models.Model):
    chan_id = models.CharField(max_length=20)
    resolution_type = models.IntegerField()
    outcome = models.IntegerField()
    outpoint_tx = models.CharField(max_length=64)
    outpoint_index = models.IntegerField()
    amount_sat = models.BigIntegerField()
    sweep_txid = models.CharField(max_length=64)
    class Meta:
        app_label = 'gui'

class PendingHTLCs(models.Model):
    chan_id = models.CharField(max_length=20)
    alias = models.CharField(max_length=32)
    incoming = models.BooleanField()
    amount = models.BigIntegerField()
    hash_lock = models.CharField(max_length=64)
    expiration_height = models.IntegerField()
    forwarding_channel = models.CharField(max_length=20)
    forwarding_alias = models.CharField(max_length=32)
    class Meta:
        app_label = 'gui'

class FailedHTLCs(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    amount = models.IntegerField()
    chan_id_in = models.CharField(max_length=20)
    chan_id_out = models.CharField(max_length=20)
    chan_in_alias = models.CharField(null=True, max_length=32)
    chan_out_alias = models.CharField(null=True, max_length=32)
    chan_out_liq = models.BigIntegerField(null=True)
    chan_out_pending = models.BigIntegerField(null=True)
    wire_failure = models.IntegerField()
    failure_detail = models.IntegerField()
    missed_fee = models.FloatField()
    class Meta:
        app_label = 'gui'

class Autopilot(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    chan_id = models.CharField(max_length=20)
    peer_alias = models.CharField(max_length=32)
    setting = models.CharField(max_length=20)
    old_value = models.IntegerField()
    new_value = models.IntegerField()
    class Meta:
        app_label = 'gui'

class Autofees(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    chan_id = models.CharField(max_length=20)
    peer_alias = models.CharField(max_length=32)
    setting = models.CharField(max_length=20)
    old_value = models.IntegerField()
    new_value = models.IntegerField()
    class Meta:
        app_label = 'gui'

class InboundFeeLog(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    chan_id = models.CharField(max_length=20)
    peer_alias = models.CharField(max_length=32)
    setting = models.CharField(max_length=20)
    old_value = models.IntegerField()
    new_value = models.IntegerField()
    class Meta:
        app_label = 'gui'

class PendingChannels(models.Model):
    funding_txid = models.CharField(max_length=64)
    output_index = models.IntegerField()
    local_base_fee = models.IntegerField(null=True, default=None)
    local_fee_rate = models.IntegerField(null=True, default=None)
    local_cltv = models.IntegerField(null=True, default=None)
    auto_rebalance = models.BooleanField(null=True, default=None)
    ar_amt_target = models.BigIntegerField(null=True, default=None)
    ar_in_target = models.IntegerField(null=True, default=None)
    ar_out_target = models.IntegerField(null=True, default=None)
    ar_max_cost = models.IntegerField(null=True, default=None)
    auto_fees = models.BooleanField(null=True, default=None)
    class Meta:
        app_label = 'gui'
        unique_together = (('funding_txid', 'output_index'),)

class AvoidNodes(models.Model):
    pubkey = models.CharField(max_length=66, primary_key=True)
    notes = models.CharField(null=True, max_length=1000)
    updated = models.DateTimeField(default=timezone.now)
    class Meta:
        app_label = 'gui'

class PeerEvents(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    chan_id = models.CharField(max_length=20)
    peer_alias = models.CharField(max_length=32)
    event = models.CharField(max_length=20)
    old_value = models.BigIntegerField(null=True)
    new_value = models.BigIntegerField()
    out_liq = models.BigIntegerField()
    class Meta:
        app_label = 'gui'

class HistFailedHTLC(models.Model):
    date = models.DateField(default=timezone.now)
    chan_id_in = models.CharField(max_length=20)
    chan_id_out = models.CharField(max_length=20)
    chan_in_alias = models.CharField(null=True, max_length=32)
    chan_out_alias = models.CharField(null=True, max_length=32)
    htlc_count = models.IntegerField()
    amount_sum = models.BigIntegerField()
    fee_sum = models.BigIntegerField()
    liq_avg = models.BigIntegerField()
    pending_avg = models.BigIntegerField()
    balance_count = models.IntegerField()
    downstream_count = models.IntegerField()
    other_count = models.IntegerField()
    class Meta:
        app_label = 'gui'
        unique_together = (('date', 'chan_id_in', 'chan_id_out'),)

class TradeSales(models.Model):
    id = models.CharField(max_length=64, primary_key=True)
    creation_date = models.DateTimeField(default=timezone.now)
    expiry = models.DateTimeField(null=True)
    description = models.CharField(max_length=100)
    price = models.BigIntegerField()
    sale_type = models.IntegerField()
    secret = models.CharField(null=True, max_length=1000)
    sale_limit = models.IntegerField(null=True)
    sale_count = models.IntegerField(default=0)


def calc_success_ratio(success_count, failure_count):
    """Return basic success ratio or an expression for ORM usage."""
    if _is_django_expression(success_count) or _is_django_expression(failure_count):
        sc = (
            Cast(success_count, FloatField())
            if _is_django_expression(success_count)
            else Value(float(success_count))
        )
        fc = (
            Cast(failure_count, FloatField())
            if _is_django_expression(failure_count)
            else Value(float(failure_count))
        )
        numerator = sc + Value(1.0)
        denominator = sc + fc + Value(2.0)
        return ExpressionWrapper(numerator / denominator, output_field=FloatField())
    return (success_count + 1) / (success_count + failure_count + 2)


def calc_weighted_ratio(success_count, failure_count, weight=10):
    """Return success ratio weighted by number of attempts."""
    if (
        _is_django_expression(success_count)
        or _is_django_expression(failure_count)
        or _is_django_expression(weight)
    ):
        sc = (
            Cast(success_count, FloatField())
            if _is_django_expression(success_count)
            else Value(float(success_count))
        )
        fc = (
            Cast(failure_count, FloatField())
            if _is_django_expression(failure_count)
            else Value(float(failure_count))
        )
        wv = weight if _is_django_expression(weight) else Value(float(weight))
        ratio = calc_success_ratio(sc, fc)
        total = Cast(sc + fc, FloatField())
        return ExpressionWrapper(
            ratio * (total / (total + wv)),
            output_field=FloatField(),
        )
    ratio = calc_success_ratio(success_count, failure_count)
    total = success_count + failure_count
    return ratio * (total / (total + weight)) if total + weight else ratio


class RebalanceRoute(models.Model):
    target_pubkey = models.CharField(max_length=66)
    outgoing_chan_id = models.CharField(max_length=20)
    # hyphen separated list of hop pubkeys (normalized path)
    route = models.TextField()
    # serialized ln.Route hex string, when available
    route_hex = models.TextField(null=True)
    final_cltv_delta = models.IntegerField(default=144)
    success_count = models.IntegerField(default=0)
    failure_count = models.IntegerField(default=0)
    last_success = models.DateTimeField(null=True, default=None)
    last_failure = models.DateTimeField(null=True, default=None)

    @property
    def success_ratio(self) -> float:
        return calc_success_ratio(self.success_count, self.failure_count)

    @property
    def weighted_ratio(self) -> float:
        """Return weighted ratio, using annotated value when available."""
        if hasattr(self, "_weighted_ratio"):
            return self._weighted_ratio
        return calc_weighted_ratio(self.success_count, self.failure_count)

    @weighted_ratio.setter
    def weighted_ratio(self, value: float):
        # allow QuerySet.annotate() to set this value without error
        self._weighted_ratio = value

    class Meta:
        app_label = 'gui'
        unique_together = (('target_pubkey', 'outgoing_chan_id', 'route'),)
        indexes = [
            models.Index(
                fields=['target_pubkey', 'outgoing_chan_id'],
                name='rebalroute_target_out_idx',
            )
        ]


class NodeCache(models.Model):
    pubkey = models.CharField(max_length=66, primary_key=True)
    data = models.JSONField()
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        app_label = 'gui'


class AmbossPeerFees(models.Model):
    pubkey = models.CharField(max_length=66, primary_key=True)
    mean_today = models.FloatField(null=True, default=None)
    median_today = models.FloatField(null=True, default=None)
    updated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        app_label = 'gui'
