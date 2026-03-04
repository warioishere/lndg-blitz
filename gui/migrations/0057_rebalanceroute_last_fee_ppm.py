from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0056_nodereputation'),
    ]

    operations = [
        migrations.AddField(
            model_name='rebalanceroute',
            name='last_fee_ppm',
            field=models.FloatField(null=True, default=None),
        ),
    ]
