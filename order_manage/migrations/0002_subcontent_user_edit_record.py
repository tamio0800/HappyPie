# Generated by Django 3.0.7 on 2020-06-29 07:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('order_manage', '0001_initial'),
    ]

    operations = [
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
