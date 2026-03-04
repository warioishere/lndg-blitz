from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0057_rebalanceroute_last_fee_ppm'),
    ]

    operations = [
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
