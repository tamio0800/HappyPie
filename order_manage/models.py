from django.db import models

class Subcontent_user_edit_record(models.Model):
    unique_id = models.TextField(null = True)
    subcontent_predict = models.TextField(null = True)
    subcontent_user_edit = models.TextField(null = True)

# 只有文字類型可以接受null
class History_data(models.Model):
    platform = models.CharField(max_length = 20)
    file_created_date = models.DateField()
    edited_shipping_date = models.DateField(null=True, blank=True)  # 修訂出貨日
    final_shipping_date = models.DateField(null=True, blank=True)  # 最終出貨日
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
    room_temperature_shipping_id = models.TextField(null = True, default='')
    low_temperature_shipping_id = models.TextField(null = True, default='')
    last_charged_date = models.TextField(null = True)
    charged = models.TextField(null = True)
    ifsend = models.BooleanField(default = False)
    ifcancel = models.BooleanField(default = False)
    vendor = models.CharField(max_length=30, null=True, blank=True, default='')
    subcontent = models.TextField(null = True)
    room_temperature_shipping_link = models.TextField(null = True, default='')
    low_temperature_shipping_link = models.TextField(null = True, default='')
    unique_id = models.TextField(null = True)
     
    # 為了使回傳platform名稱而不是object
    def __str__(self):
        return f'{self.platform} - {self.txn_id}'


class Qingye_Niancai_raw_record(models.Model):
    '''
    將符合2020青葉年菜grouping機制的交易紀錄留一份在這裡
    '''
    txn_id = models.CharField(max_length = 60)
    vendor = models.CharField(max_length=30, null=True, blank=True, default='')
    content = models.TextField(null = True)
     
    # 為了使回傳platform名稱而不是object
    def __str__(self):
        return f'{self.platform} - {self.txn_id}'
