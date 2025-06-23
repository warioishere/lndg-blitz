from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0047_channels_mx_liq_override'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='mx_liq_upper',
            field=models.BigIntegerField(default=0),
        ),
    ]

