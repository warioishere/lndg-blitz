from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0054_add_indexes'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='htlc_boost_checked',
            field=models.DateTimeField(null=True, default=None),
        ),
    ]
