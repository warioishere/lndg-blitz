from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0044_indexes'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='inbound_offset',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='channels',
            name='offset_updated',
            field=models.DateTimeField(null=True, default=None),
        ),
    ]
