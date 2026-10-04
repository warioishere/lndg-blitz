from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0061_graphprobelog_restore_probelog'),
    ]

    operations = [
        migrations.AddField(
            model_name='payments',
            name='source_fee_rate',
            field=models.IntegerField(null=True),
        ),
    ]
