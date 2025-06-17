from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0042_nodecache'),
    ]

    operations = [
        migrations.AddField(
            model_name='rebalancer',
            name='allow_source',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='rebalancer',
            name='allowed_targets',
            field=models.TextField(default='[]'),
        ),
    ]
