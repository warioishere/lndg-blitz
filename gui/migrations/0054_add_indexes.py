from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('gui', '0053_flp_enabled'),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gui_channels_rebalance "
                "ON gui_channels (is_active, is_open, auto_rebalance);"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS idx_gui_channels_rebalance;",
        ),
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gui_channels_chan_id "
                "ON gui_channels (chan_id);"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS idx_gui_channels_chan_id;",
        ),
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gui_rebalancer_status "
                "ON gui_rebalancer (status);"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS idx_gui_rebalancer_status;",
        ),
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gui_rebalancer_last_hop "
                "ON gui_rebalancer (last_hop_pubkey);"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS idx_gui_rebalancer_last_hop;",
        ),
    ]

