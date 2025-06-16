from django.db import migrations, models
from django.utils import timezone

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0041_rebalanceroute_upgrade'),
    ]

    operations = [
        migrations.CreateModel(
            name='NodeCache',
            fields=[
                ('pubkey', models.CharField(primary_key=True, max_length=66, serialize=False)),
                ('data', models.JSONField()),
                ('updated_at', models.DateTimeField(default=timezone.now)),
            ],
            options={'app_label': 'gui'},
        ),
    ]
