import pandas as pd, numpy as np
from .models import History_data
# 存已合併訂單進DB
# 判斷該筆訂單是否已經存在於DB
# 如果aggregate的ifsend, ifcancel 都= 0 而資料庫!= 0的話以資料庫為準

# 讀'合併訂單' 檔成dataframe
#df = pd.read_excel('order_manage/con_data/output_uniq.xlsx')
df = pd.read_excel('order_manage/con_data/output_uniq_fake.xlsx') #測試用

# 讀取後將中文col_name轉成和db table一樣的colname
df.columns = ['platform', 'file_created_date', 'txn_id', 'customer_name', 'receiver_name',
    'paid_after_receiving', 'receiver_phone_nbr', 'receiver_mobile',
    'receiver_address', 'content',  'how_many','how_much', 'remark', 
    'shipping_id', 'last_charged_date', 'charged', 
    'ifsend', 'ifcancel','subcontent', 'shipping_link','unique_id']

df.file_created_date = pd.to_datetime(df.file_created_date).dt.date.astype(str)
df.last_charged_date = df.last_charged_date.apply(lambda x: '' if pd.isnull(x) else str(x))
for _ in ['paid_after_receiving', 'ifsend', 'ifcancel', 'charged']:
    df[_] = df[_].apply(lambda x: False if (pd.isnull(x) or x is None) else x)

for _ in ['how_many','how_much']:
    df[_] = df[_].apply(lambda x: 0 if (pd.isnull(x) or x is None) else x)

# 如果合併訂單的 uni_id跟資料庫裡的一樣，表示資料已存在
# 則接著判斷寄出、取消狀態   aggregate的ifsend, ifcancel 會 = 0 而資料庫!= 0的話以資料庫為準
for ids in df['unique_id']: 
    if History_data.objects.filter(unique_id = ids):
        if df[df['unique_id'] == ids]['ifsend'].tolist()[0]:
        # History_data.objects.filter(unique_id = ids) 的 ifsend
        History_data.objects.filter(unique_id = ids).update(ifsend = df[df['unique_id'] == ids]['ifsend'].tolist()[0])
        History_data.objects.filter(unique_id = ids).update(ifcancel = df[df['unique_id'] == ids]['ifcancel'].tolist()[0])
        History_data.objects.filter(unique_id = ids).update(shipping_id = df[df['unique_id'] == ids]['shipping_id'].tolist()[0])
        print('已更新資料庫狀態:'+ ids )
    
    # 資料庫沒有這筆資料，要新增
    else:
        temp_platform = df[df['unique_id'] == ids]['platform'].tolist()[0]
        temp_file_created_date = df[df['unique_id'] == ids]['file_created_date'].tolist()[0]
        temp_txn_id = df[df['unique_id'] == ids]['txn_id'].tolist()[0]
        temp_customer_name = df[df['unique_id'] == ids]['customer_name'].tolist()[0]
        temp_receiver_name = df[df['unique_id'] == ids]['receiver_name'].tolist()[0]
        temp_paid_after_receiving = df[df['unique_id'] == ids]['paid_after_receiving'].tolist()[0]
        temp_receiver_phone_nbr = df[df['unique_id'] == ids]['receiver_phone_nbr'].tolist()[0]
        temp_receiver_mobile = df[df['unique_id'] == ids]['receiver_mobile'].tolist()[0]
        temp_receiver_address = df[df['unique_id'] == ids]['receiver_address'].tolist()[0]
        temp_content = df[df['unique_id'] == ids]['content'].tolist()[0]
        temp_how_many = df[df['unique_id'] == ids]['how_many'].tolist()[0]
        temp_how_much = df[df['unique_id'] == ids]['how_much'].tolist()[0]
        temp_remark = df[df['unique_id'] == ids]['remark'].tolist()[0]
        temp_shipping_id = df[df['unique_id'] == ids]['shipping_id'].tolist()[0]
        temp_last_charged_date = df[df['unique_id'] == ids]['last_charged_date'].tolist()[0]
        temp_charged = df[df['unique_id'] == ids]['charged'].tolist()[0]
        temp_ifsend = df[df['unique_id'] == ids]['ifsend'].tolist()[0]
        temp_ifcancel = df[df['unique_id'] == ids]['ifcancel'].tolist()[0]
        temp_subcontent = df[df['unique_id'] == ids]['subcontent'].tolist()[0]
        temp_shipping_link = df[df['unique_id'] == ids]['shipping_link'].tolist()[0]
        temp_unique_id = df[df['unique_id'] == ids]['unique_id'].tolist()[0]

        
        print('新增訂單 : '+ ids)
        History_data(unique_id = ids, platform = temp_platform, 
                    file_created_date = temp_file_created_date,
                    txn_id = temp_txn_id  ,
                    customer_name = temp_customer_name  ,
                    receiver_name = temp_receiver_name  ,
                    paid_after_receiving  = temp_paid_after_receiving,
                    receiver_phone_nbr = temp_receiver_phone_nbr ,
                    receiver_mobile  = temp_receiver_mobile  ,
                    receiver_address = temp_receiver_address,
                    content = temp_content ,
                    how_many = temp_how_many  ,
                    how_much  = temp_how_much,
                    remark = temp_remark  ,
                    shipping_id  = temp_shipping_id  ,
                    last_charged_date  = temp_last_charged_date ,
                    charged = temp_charged ,
                    ifsend  = temp_ifsend , 
                    ifcancel = temp_ifcancel  ,
                    subcontent  = temp_subcontent  ,
                    shipping_link = temp_shipping_link).save()

        #temp_history_data = History_data()
