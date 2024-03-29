
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='History_data',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('platform', models.CharField(max_length=20)),
                ('file_created_date', models.DateField()),
                ('edited_shipping_date', models.DateField(blank=True, null=True)),
                ('final_shipping_date', models.DateField(blank=True, null=True)),
                ('txn_id', models.CharField(max_length=60)),
                ('customer_name', models.CharField(max_length=20, null=True)),
                ('receiver_name', models.CharField(max_length=20, null=True)),
                ('paid_after_receiving', models.BooleanField(default=False)),
                ('receiver_address', models.CharField(max_length=60)),
                ('receiver_phone_nbr', models.CharField(max_length=20, null=True)),
                ('receiver_mobile', models.CharField(max_length=20, null=True)),
                ('content', models.TextField(null=True)),
                ('how_much', models.IntegerField(default=False)),
                ('how_many', models.IntegerField(default=False)),
                ('remark', models.TextField(null=True)),
                ('room_temperature_shipping_id', models.TextField(default='', null=True)),
                ('low_temperature_shipping_id', models.TextField(default='', null=True)),
                ('last_charged_date', models.TextField(null=True)),
                ('charged', models.TextField(null=True)),
                ('ifsend', models.BooleanField(default=False)),
                ('ifcancel', models.BooleanField(default=False)),
                ('vendor', models.CharField(blank=True, default='', max_length=30, null=True)),
                ('subcontent', models.TextField(null=True)),
                ('room_temperature_shipping_link', models.TextField(default='', null=True)),
                ('low_temperature_shipping_link', models.TextField(default='', null=True)),
                ('unique_id', models.TextField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Qingye_Niancai_raw_record',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('txn_id', models.CharField(max_length=60)),
                ('vendor', models.CharField(blank=True, default='', max_length=30, null=True)),
                ('content', models.TextField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Subcontent_user_edit_record',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('unique_id', models.TextField(null=True)),
                ('subcontent_predict', models.TextField(null=True)),
                ('subcontent_user_edit', models.TextField(null=True)),
            ],
        ),
    ]
