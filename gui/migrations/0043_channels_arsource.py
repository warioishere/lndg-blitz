from django.db import migrations, models
import django.db.models.deletion

class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0042_nodecache'),
    ]

    operations = [
        migrations.AddField(
            model_name='channels',
            name='ar_source',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='channels',
            name='ar_source_ppm_diff',
            field=models.IntegerField(default=0),
        ),
        migrations.CreateModel(
            name='AllowedTarget',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('target_pubkey', models.CharField(max_length=66)),
                ('source_chan', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gui.channels')),
            ],
            options={
                'app_label': 'gui',
                'unique_together': {('source_chan', 'target_pubkey')},
            },
        ),
    ]
