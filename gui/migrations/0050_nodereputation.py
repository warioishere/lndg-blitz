from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0049_localsettings_long_value'),
    ]

    operations = [
        migrations.CreateModel(
            name='NodeReputation',
            fields=[
                ('pubkey', models.CharField(max_length=66, primary_key=True, serialize=False)),
                ('success_count', models.IntegerField(default=0)),
                ('failure_count', models.IntegerField(default=0)),
                ('last_success', models.DateTimeField(null=True)),
                ('last_failure', models.DateTimeField(null=True)),
            ],
            options={
                'app_label': 'gui',
            },
        ),
    ]
