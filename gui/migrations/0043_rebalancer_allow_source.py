from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0042_nodecache'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='ar_allow_source',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='channels',
            name='ar_allowed_targets',
            field=models.TextField(default='[]'),
        ),
        migrations.AddField(
            model_name='channels',
            name='ar_source_margin',
            field=models.IntegerField(default=100),
        ),
    ]
