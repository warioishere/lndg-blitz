from django.db import migrations, models
import django.utils.timezone

class Migration(migrations.Migration):
    dependencies = [
        ('gui', '0051_add_emergency_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='ep_enabled',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='channels',
            name='ep_inc_pct',
            field=models.FloatField(default=10),
        ),
        migrations.AddField(
            model_name='channels',
            name='ep_cooldown',
            field=models.IntegerField(default=10),
        ),
        migrations.AddField(
            model_name='channels',
            name='ep_live_threshold',
            field=models.IntegerField(default=40),
        ),
        migrations.AddField(
            model_name='channels',
            name='ep_live_inc_pct',
            field=models.FloatField(default=5),
        ),
    ]
