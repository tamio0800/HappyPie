# Generated by Django 3.0.6 on 2020-06-17 06:35

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
                ('shipping_id', models.TextField(null=True)),
                ('last_charged_date', models.TextField(null=True)),
                ('charged', models.TextField(null=True)),
                ('ifsend', models.BooleanField(default=False)),
                ('ifcancel', models.BooleanField(default=False)),
                ('subcontent', models.TextField(null=True)),
                ('shipping_link', models.TextField(null=True)),
                ('unique_id', models.TextField(null=True)),
            ],
        ),
    ]
