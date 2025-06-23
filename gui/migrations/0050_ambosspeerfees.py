from django.db import migrations, models
import django.utils.timezone

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0049_localsettings_long_value'),
    ]

    operations = [
        migrations.CreateModel(
            name='AmbossPeerFees',
            fields=[
                ('pubkey', models.CharField(max_length=66, primary_key=True, serialize=False)),
                ('mean_today', models.FloatField(null=True, default=None)),
                ('median_today', models.FloatField(null=True, default=None)),
                ('updated_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'app_label': 'gui',
            },
        ),
    ]
