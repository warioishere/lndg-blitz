from django.db import migrations, models
import django.utils.timezone

class Migration(migrations.Migration):
    dependencies = [
        ('gui', '0050_ambosspeerfees'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='ep_target',
            field=models.IntegerField(default=50),
        ),
        migrations.AddField(
            model_name='channels',
            name='ep_updated',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
    ]
