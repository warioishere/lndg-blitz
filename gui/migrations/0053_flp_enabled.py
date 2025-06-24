from django.db import migrations, models

class Migration(migrations.Migration):
    dependencies = [
        ('gui', '0052_ep_channel_settings'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='flp_enabled',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='channels',
            name='flp_safety',
            field=models.IntegerField(default=0),
        ),
    ]
