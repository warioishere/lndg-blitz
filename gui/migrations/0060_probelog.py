from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0059_graphevent'),
    ]

    operations = [
        # 0058 created an (unrelated) ProbeLog table; drop it first so this CreateModel
        # works on a fresh database. On existing deployments this migration was already
        # applied (after a manual DROP + fake), so it never re-runs here.
        migrations.DeleteModel(name='ProbeLog'),
        migrations.CreateModel(
            name='ProbeLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.DateTimeField(default=django.utils.timezone.now)),
                ('target_pubkey', models.CharField(max_length=66)),
                ('target_alias', models.CharField(default='', max_length=64)),
                ('target_fee', models.IntegerField(default=0)),
                ('target_max_cost', models.IntegerField(default=0)),
                ('trigger_chan_id', models.CharField(default='', max_length=20)),
                ('other_pubkey', models.CharField(default='', max_length=66)),
                ('other_alias', models.CharField(default='', max_length=64)),
                ('other_fee_ppm', models.IntegerField(null=True)),
                ('budget_ppm', models.IntegerField(default=0)),
                ('sources_tried', models.IntegerField(default=0)),
                ('routes_new', models.IntegerField(default=0)),
                ('routes_existing', models.IntegerField(default=0)),
                ('errors', models.IntegerField(default=0)),
                ('routes_via_new_peer', models.IntegerField(default=0)),
                ('rebalance_scheduled', models.BooleanField(default=False)),
                ('details', models.TextField(default='')),
            ],
            options={
                'ordering': ['-timestamp'],
            },
        ),
    ]
