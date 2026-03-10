import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0058_probelog'),
    ]

    operations = [
        migrations.CreateModel(
            name='GraphEvent',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.DateTimeField(default=django.utils.timezone.now)),
                ('event_type', models.CharField(max_length=30)),
                ('chan_id', models.CharField(max_length=20)),
                ('capacity', models.BigIntegerField(default=0)),
                ('fee_ppm', models.IntegerField(null=True)),
                ('base_fee_msat', models.BigIntegerField(default=0)),
                ('target_pubkey', models.CharField(max_length=66)),
                ('target_alias', models.CharField(max_length=32)),
                ('other_node', models.CharField(default='', max_length=66)),
                ('other_alias', models.CharField(default='', max_length=32)),
                ('disabled', models.BooleanField(default=False)),
                ('probe_triggered', models.BooleanField(default=False)),
                ('routes_found', models.IntegerField(default=0)),
                ('policy_node', models.CharField(default='', max_length=66)),
            ],
            options={
                'app_label': 'gui',
                'ordering': ['-timestamp'],
                'indexes': [
                    models.Index(fields=['target_pubkey'], name='graphevent_target_idx'),
                ],
            },
        ),
    ]
