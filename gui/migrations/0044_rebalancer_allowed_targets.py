from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0043_rebalancer_allow_source'),
    ]

    operations = [
        migrations.AddField(
            model_name='rebalancer',
            name='allowed_targets',
            field=models.TextField(default='[]'),
        ),
    ]
