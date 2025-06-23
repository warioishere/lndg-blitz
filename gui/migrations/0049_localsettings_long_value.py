from django.db import migrations, models

# Migration needed because LocalSettings.value was originally a
# CharField(max_length=50). Amboss API tokens can exceed 250 characters,
# so we switch to TextField to avoid truncation errors.

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0048_channels_mx_liq_upper'),
    ]

    operations = [
        migrations.AlterField(
            model_name='localsettings',
            name='value',
            field=models.TextField(default=None),
        ),
    ]
