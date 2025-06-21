from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0046_channels_maxhtlc_percent'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='mx_liq_threshold',
            field=models.BigIntegerField(default=0),
        ),
        migrations.AddField(
            model_name='channels',
            name='mx_liq_value',
            field=models.BigIntegerField(default=0),
        ),
    ]

