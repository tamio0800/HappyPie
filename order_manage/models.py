from django.db import models

class Subcontent_user_edit_record(models.Model):
    unique_id = models.TextField(null = True)
    subcontent_predict = models.TextField(null = True)
    subcontent_user_edit = models.TextField(null = True)

# 只有文字類型可以接受null
class History_data(models.Model):
    platform = models.CharField(max_length = 20)
    file_created_date = models.DateField()
    txn_id = models.CharField(max_length = 60)
    customer_name = models.CharField(max_length = 20, null = True)
    receiver_name = models.CharField(max_length = 20, null = True,)
    paid_after_receiving = models.BooleanField(default = False)
    receiver_address = models.CharField(max_length = 60)
    receiver_phone_nbr = models.CharField(max_length = 20, null = True)
    receiver_mobile = models.CharField(max_length = 20, null = True)
    content = models.TextField(null = True)
    how_much = models.IntegerField(default = False)
    how_many = models.IntegerField(default = False)
    remark = models.TextField(null = True)
    shipping_id = models.TextField(null = True)
    last_charged_date = models.TextField(null = True)
    charged = models.TextField(null = True)
    ifsend = models.BooleanField(default = False)
    ifcancel = models.BooleanField(default = False)
    subcontent = models.TextField(null = True)
    shipping_link = models.TextField(null = True)
    unique_id = models.TextField(null = True)
     
    # 為了使回傳platform名稱而不是object
    def __str__(self):
        return self.platform



##class test_db(models.Model):
 #   t_date = models.CharField(max_length=35, default='')
#    Volatage = models.FloatField(default=0)
 #   Current = models.FloatField(default=0)
 #   Power = models.FloatField(default=0)