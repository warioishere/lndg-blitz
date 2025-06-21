from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0045_channels_inbound_offset'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='maxhtlc_percent',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='channels',
            name='maxhtlc_updated',
            field=models.DateTimeField(null=True, default=None),
        ),
    ]
