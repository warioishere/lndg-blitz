from django.db import migrations, models


class Migration(migrations.Migration):
    """Rename the Graph Watcher's ProbeLog to GraphProbeLog (it shared a
    name with the older ProbeLog used by probe_routes_job which broke the
    rebalance-routes probing tab). Then re-create the original ProbeLog
    table so the background job can log probe history again.

    Note: the original ProbeLog table was dropped by the earlier manual
    recovery (DROP TABLE + fake migrate). Historical rows are lost;
    probe_routes_job will refill on its next run.
    """

    dependencies = [
        ('gui', '0060_probelog'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='ProbeLog',
            new_name='GraphProbeLog',
        ),
        migrations.CreateModel(
            name='ProbeLog',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.DateTimeField(auto_now_add=True)),
                ('targets_scanned', models.IntegerField(default=0)),
                ('routes_found', models.IntegerField(default=0)),
                ('routes_existing', models.IntegerField(default=0)),
                ('errors', models.IntegerField(default=0)),
                ('duration_ms', models.IntegerField(default=0)),
                ('details', models.JSONField(default=list)),
            ],
            options={
                'app_label': 'gui',
                'ordering': ['-timestamp'],
            },
        ),
    ]
