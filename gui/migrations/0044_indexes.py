from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gui', '0043_channels_arsource'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='forwards',
            index=models.Index(fields=['forward_date'], name='forwards_date_idx'),
        ),
        migrations.AddIndex(
            model_name='forwards',
            index=models.Index(fields=['chan_id_in'], name='forwards_in_idx'),
        ),
        migrations.AddIndex(
            model_name='forwards',
            index=models.Index(fields=['chan_id_out'], name='forwards_out_idx'),
        ),
        migrations.AddIndex(
            model_name='forwards',
            index=models.Index(fields=['chan_id_in', 'forward_date'], name='forwards_in_date_idx'),
        ),
        migrations.AddIndex(
            model_name='forwards',
            index=models.Index(fields=['chan_id_out', 'forward_date'], name='forwards_out_date_idx'),
        ),
        migrations.AddIndex(
            model_name='payments',
            index=models.Index(fields=['index'], name='payments_index_idx'),
        ),
        migrations.AddIndex(
            model_name='payments',
            index=models.Index(fields=['chan_out'], name='payments_chan_out_idx'),
        ),
        migrations.AddIndex(
            model_name='payments',
            index=models.Index(fields=['rebal_chan'], name='payments_rebal_chan_idx'),
        ),
        migrations.AddIndex(
            model_name='paymenthops',
            index=models.Index(fields=['chan_id'], name='paymenthops_chanid_idx'),
        ),
        migrations.AddIndex(
            model_name='paymenthops',
            index=models.Index(fields=['node_pubkey'], name='paymenthops_node_pubkey_idx'),
        ),
    ]

