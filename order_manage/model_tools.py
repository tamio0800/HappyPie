import pandas as pd, numpy as np
from .models import History_data
from django_pandas.io import read_frame
from .models import Subcontent_user_edit_record
pd.options.mode.chained_assignment = None

class HISTORY_DATA_and_Subcontent_user_edit_record_db_writer:

    def __init__(self, **kwargs):   
        if 'dataframe_path' in kwargs.keys():
            self.dataframe = pd.read_excel(kwargs['dataframe_path'])
            print('model_tools', 'got df path.')
            # self.dataframe = 1
        elif 'dataframe' in kwargs.keys():
            self.dataframe = kwargs['dataframe']
            print('model_tools', 'got df.')
            # self.dataframe = 2
        else:
            raise 'ERROR! Has Not Input Dataframe.'

        self.column_names_dict = {
            '通路':'platform',
            '抓單日':'file_created_date',
            '訂單編號':'txn_id',
            '訂購人':'customer_name',
            '收件人':'receiver_name',
            '貨到付款':'paid_after_receiving',
            '電話':'receiver_phone_nbr',
            '手機':'receiver_mobile',
            '地址':'receiver_address',
            '內容物':'content',
            '數量':'how_many',
            '金額':'how_much',
            '備註':'remark',
            '宅單':'shipping_id',
            '最後回押日':'last_charged_date',
            '回押':'charged',
            '已寄出':'ifsend',
            '已取消':'ifcancel',
            '規格':'subcontent',
            '貨運連結':'shipping_link',
            'unique_id':'unique_id',
        }


    def _check_dataframe(self):
        # 確認該dataframe符合我們的格式
        print('_check_dataframe')
        print(self.dataframe.columns)
        print(self.dataframe.head())
        if 'unique_id' not in self.dataframe.columns:
            assert len(self.dataframe.columns) == 20
            assert sorted(list(self.dataframe.columns)) == sorted(list(self.column_names_dict.keys())[:-1])
            self.dataframe.loc[:, 'unique_id'] = self.dataframe['通路'] + '-' + self.dataframe['訂單編號'].astype(str)
        else:
            assert len(self.dataframe.columns) == 21
            assert sorted(list(self.dataframe.columns)) == sorted(list(self.column_names_dict.keys()))

        print('hddw1', 'check done.')
            

    def _make_dataframe_columns_to_match_db_columns(self):
        self.dataframe.columns = [self.column_names_dict[_] for _ in self.dataframe.columns]
        # 將DB Table的column name置換原本對應的中文column name

        for each_col in self.dataframe.columns:
            try:
                if each_col not in ['how_many','how_much']:
                    self.dataframe[each_col] = self.dataframe[each_col].apply(lambda x: x.strip())
            except:
                pass

        self.dataframe.file_created_date = \
            pd.to_datetime(self.dataframe.file_created_date).dt.date.astype(str)
        self.dataframe.last_charged_date = \
            self.dataframe.last_charged_date.apply(lambda x: '' if pd.isnull(x) else str(x))

        for _ in ['paid_after_receiving', 'ifsend', 'ifcancel', 'charged']:
            self.dataframe[_] = self.dataframe[_].apply(lambda x: False if (pd.isnull(x) or x == 'FALSE' or x == 'N' or x == 0 or x == False or x is None) else True)

        for _ in ['how_many','how_much']:
            self.dataframe[_] = self.dataframe[_].apply(lambda x: 0 if (pd.isnull(x) or x is None or x == '') else x)

        for _ in ['shipping_id']:
            self.dataframe[_] = self.dataframe[_].astype(str)

        print('hddw2', 'make_dataframe_columns_to_match_db_columns done.')


    def query_all_pending_txns(self):
        # 找出還未處理完(未取消也未出貨)的訂單, 並以pandas.dataframe形式回傳
        #pending_txns_objects = History_data.objects.raw('\
        #    select * from order_manage_History_data where ifsend = FALSE and ifcancel = FALSE;\')
        pending_txns_objects = History_data.objects.filter(ifsend=False, ifcancel=False).all()
        dataframe_tobe_returned = read_frame(pending_txns_objects).drop(['unique_id', 'id'], axis=1)

        mandarin_column_names = []
        for each_eng_column in dataframe_tobe_returned.columns:
            for k, v in self.column_names_dict.items():
                if v == each_eng_column:
                    mandarin_column_names.append(k)
                    break
        dataframe_tobe_returned.columns = mandarin_column_names
        return dataframe_tobe_returned


    def english_db_column_names_to_mandarin(self):
        mandarin_column_names = []
        for each_eng_column in self.dataframe.columns:
            for k, v in self.column_names_dict.items():
                if v == each_eng_column:
                    mandarin_column_names.append(k)
                    break
        self.dataframe.columns = mandarin_column_names

    def generate_shipping_link(self, shipping_id):
        _temp_logistic_company = None
        if shipping_id is None or shipping_id == '':
            return ''
        
        try:
            _temp_shipping_id = shipping_id
            if len(_temp_shipping_id) == 10:
                # 新竹物流的貨運編號長度為10，黑貓的長度為12
                _temp_logistic_company = 'xinzhu'
            elif len(_temp_shipping_id) == 12:
                _temp_logistic_company = 'black_cat'
        except Exception as e:
            print('generate_shipping_link ERROR: ', e)
            pass
        
        if _temp_logistic_company is not None:
            shipping_link = 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(_temp_shipping_id) + '&logistic_company=' + _temp_logistic_company
        #elif _temp_logistic_company is None and len(_temp_shipping_id) > 0:
        #    shipping_link = ''
        else:
            shipping_link = ''
        
        return shipping_link


    def write_in_2diff_db(self):
        # 這些是允許user修改的column
        def History_data_update(ids):
            df_correspondant_index = self.dataframe.index[self.dataframe['unique_id']==ids][0]  
            # get df's correspondant index to prevent from looping querying.
            txn_object = History_data.objects.get(unique_id = ids)
            # get txn_object to prevent from looping ORM filtering. 
            txn_object.ifsend = self.dataframe.loc[df_correspondant_index]['ifsend']
            txn_object.ifcancel = self.dataframe.loc[df_correspondant_index]['ifcancel']
            txn_object.shipping_id = self.dataframe.loc[df_correspondant_index]['shipping_id']
            txn_object.subcontent = self.dataframe.loc[df_correspondant_index]['subcontent']
            # 新增欄位，讓user可以修改的欄位增加
            # 其中一個原因是2020.08.04時曉箐反映有時客戶指定到貨日期時，
            # 她們會如此註記 周文斌 >> 周文斌(12/25到貨)
            # (曉箐原話:) 廠商不看備註!!他們都是直接看收件人的名字跟商品內容
            txn_object.customer_name = self.dataframe.loc[df_correspondant_index]['customer_name']
            txn_object.receiver_name = self.dataframe.loc[df_correspondant_index]['receiver_name']
            txn_object.content = self.dataframe.loc[df_correspondant_index]['content']
            txn_object.how_many = self.dataframe.loc[df_correspondant_index]['how_many']
            # txn_object.file_created_date = self.dataframe.loc[df_correspondant_index]['file_created_date']
            print('History_data_update_1 Done: ', ids)
            # 在這裡寫上產出貨運連結的程式碼
            _temp_logistic_company = None
            try:
                _temp_shipping_id = self.dataframe.loc[df_correspondant_index]['shipping_id']
                print('write_in_db-1.2', _temp_shipping_id)
                if len(_temp_shipping_id) == 10:
                    # 新竹物流的貨運編號長度為10，黑貓的長度為12
                    _temp_logistic_company = 'xinzhu'
                elif len(_temp_shipping_id) == 12:
                    _temp_logistic_company = 'black_cat'
                print('write_in_db-1.3', _temp_logistic_company)
            except:
                pass
            print('History_data_update_2 Done')
            if _temp_logistic_company is not None:
                #print(History_data.objects.filter(unique_id = ids).shipping_id)
                print('History_data_update_3.1: ', _temp_shipping_id)
                txn_object.shipping_id = _temp_shipping_id
                # History_data.objects.filter(unique_id = ids).update(shipping_id = _temp_shipping_id)
                #print(History_data.objects.filter(unique_id = ids).shipping_id)
                txn_object.shipping_link = 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(_temp_shipping_id) + '&logistic_company=' + _temp_logistic_company
                # History_data.objects.filter(unique_id = ids).update(shipping_link = 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(_temp_shipping_id) + '&logistic_company=' + _temp_logistic_company)
            elif _temp_logistic_company is None and len(_temp_shipping_id) > 0:
                print('History_data_update_3.1: ', _temp_shipping_id)
                txn_object.shipping_id = _temp_shipping_id
                txn_object.shipping_link = ''
                # History_data.objects.filter(unique_id = ids).update(shipping_link='')
            print('已更新history_data:'+ ids )
            print('History_data_update_3 Done')
            txn_object.save()

        self._check_dataframe()
        self._make_dataframe_columns_to_match_db_columns()
        # print('write_in_2diff_db_1 Done')
        # print(self.dataframe.head(1).T)
        # print(self.dataframe.info())
        # print(self.dataframe.columns)

        # 如果合併訂單的 uni_id跟資料庫裡的一樣，表示資料已存在
        # 則接著更新寄出、取消狀態
        # self.dataframe.to_excel("XXXXXXXXXXX.xlsx", index=False)
        for each_id in self.dataframe['unique_id']: 
            print('write_in_2diff_db_2', each_id)
            df_correspondant_index = self.dataframe[self.dataframe['unique_id']==each_id].index[0]
            
            
            history_data_object = History_data.objects.filter(unique_id = each_id).first()
            if history_data_object is not None:
                # History_data 資料庫已有這筆資料
                print('write_in_2diff_db_2.1: record(' + each_id + ') is in database.')
                if not pd.isnull(self.dataframe.loc[df_correspondant_index]['shipping_link']) and len(self.dataframe.loc[df_correspondant_index]['shipping_link']) > 0:
                    _shipping_link = self.dataframe.loc[df_correspondant_index]['shipping_link']
                else:
                    _shipping_link = self.generate_shipping_link(self.dataframe.loc[df_correspondant_index]['shipping_id'])
                print('write_in_2diff_db_2.2: SHIPPING LINK  >>  ', _shipping_link)
                print('write_in_2diff_db_2.3: df_correspondant_index  >>  ', df_correspondant_index)
                print('write_in_2diff_db_2.4: txn_id  >>  ', self.dataframe.loc[df_correspondant_index]['txn_id'])
                # for each_col in ['txn_id', 'customer_name', 'receiver_name', 'paid_after_receiving', 
                # 'receiver_address', 'receiver_phone_nbr', 'receiver_mobile', 'content', 'how_much', 
                # 'how_many', 'remark', 'shipping_id', 'last_charged_date', 'charged', 'ifsend', 'ifcancel', 'subcontent']:
                #     print('Test Column:  ', each_col)
                #     print(self.dataframe.loc[df_correspondant_index][each_col])

                history_data_object.txn_id = self.dataframe.loc[df_correspondant_index]['txn_id']
                history_data_object.file_created_date = self.dataframe.loc[df_correspondant_index]['file_created_date']
                history_data_object.customer_name = self.dataframe.loc[df_correspondant_index]['customer_name']
                history_data_object.receiver_name = self.dataframe.loc[df_correspondant_index]['receiver_name']
                history_data_object.paid_after_receiving = self.dataframe.loc[df_correspondant_index]['paid_after_receiving']
                history_data_object.receiver_address = self.dataframe.loc[df_correspondant_index]['receiver_address']
                history_data_object.receiver_phone_nbr = self.dataframe.loc[df_correspondant_index]['receiver_phone_nbr']
                history_data_object.receiver_mobile = self.dataframe.loc[df_correspondant_index]['receiver_mobile']
                history_data_object.content = self.dataframe.loc[df_correspondant_index]['content']
                history_data_object.how_much = self.dataframe.loc[df_correspondant_index]['how_much']
                history_data_object.how_many = self.dataframe.loc[df_correspondant_index]['how_many']
                history_data_object.remark = self.dataframe.loc[df_correspondant_index]['remark']
                history_data_object.shipping_id = self.dataframe.loc[df_correspondant_index]['shipping_id']
                history_data_object.last_charged_date = self.dataframe.loc[df_correspondant_index]['last_charged_date']
                history_data_object.charged = self.dataframe.loc[df_correspondant_index]['charged']
                history_data_object.ifsend = self.dataframe.loc[df_correspondant_index]['ifsend']
                history_data_object.ifcancel = self.dataframe.loc[df_correspondant_index]['ifcancel']
                history_data_object.shipping_link = _shipping_link

            
                print('write_in_2diff_db_2.4: history_data_object has updated.')

                if history_data_object.subcontent != self.dataframe.loc[df_correspondant_index]['subcontent']:
                    # 規格欄位有被修改過，需要更新追蹤user將規格從什麼改成什麼
                    subcontent_edit_history_object = Subcontent_user_edit_record.objects.filter(unique_id=each_id).first()
                    if subcontent_edit_history_object is not None:
                        subcontent_edit_history_object.subcontent_user_edit = self.dataframe.loc[df_correspondant_index]['subcontent']
                    else:
                        _former_predicted_subcontent = history_data_object.subcontent  # 我們原本產出的規格
                        _user_edited_subcontent = self.dataframe.loc[df_correspondant_index]['subcontent']  # user 新改的規格
                        Subcontent_user_edit_record.objects.create(
                            subcontent_predict = _former_predicted_subcontent,
                            subcontent_user_edit = _user_edited_subcontent
                        ).save()
                        history_data_object.subcontent = self.dataframe.loc[df_correspondant_index]['subcontent']


                # if history_data_object.filter(subcontent = self.dataframe.loc[df_correspondant_id]['subcontent']).first() is not None:
                # if History_data.objects.filter(unique_id = ids).filter(subcontent = self.dataframe.loc[df_correspondant_id]['subcontent']).count() == 1:
                #     print('write_in_2diff_db_2.1.1: subcontent is same as df[subcontent]')
                #     History_data_update(ids)
                # 如果資料庫中的subcontent跟新來的df[subcontent]不一致,表示user有修改過subcontent,
                # 為了追蹤修改的軌跡把資料存到另一個DB:subcontent_user_edit_record
                # else: 
                    # 如果這筆已經存在於修改規格紀錄DB, 表示user不是第一次改變subcontent, 修改規格紀錄DB只要update最新的就好
                #    print('write_in_2diff_db_2.1.1: subcontent is different from df[subcontent]')
                #    if Subcontent_user_edit_record.objects.filter(unique_id = ids).count() == 1:
                #        Subcontent_user_edit_record.objects.filter(unique_id = ids).update(subcontent_user_edit = self.dataframe.loc[df_correspondant_id]['subcontent'])
                #        print('update Subcontent_user_edit_record : '+ ids)
                #        History_data_update(ids) # 更新追蹤DB之後也要更新歷史訂單DB
                    # 如果這筆不存在於修改規格紀錄DB, 則新增
                #     else: 
                #         temp_subcontent_predict = History_data.objects.filter(unique_id = ids).values('subcontent') # 我們原本產出的規格
                #         temp_user_edit = self.dataframe.loc[df_correspondant_id]['subcontent'] # user 新改的規格
                #        Subcontent_user_edit_record(unique_id = ids, 
                #                                     subcontent_predict = temp_subcontent_predict,
                #                                     subcontent_user_edit = temp_user_edit).save()
                      # print('add Subcontent_user_edit_record : '+ ids) 
                #         History_data_update(ids) # 新增到追蹤DB之後也要更新歷史訂單DB
            
            else:

                # 資料庫沒有這筆資料，要新增
                print('write_in_2diff_db_2.1: not in db.')
                temp_platform = self.dataframe.loc[df_correspondant_index]['platform']
                temp_file_created_date = self.dataframe.loc[df_correspondant_index]['file_created_date']
                temp_txn_id = self.dataframe.loc[df_correspondant_index]['txn_id']
                temp_customer_name = self.dataframe.loc[df_correspondant_index]['customer_name']
                temp_receiver_name = self.dataframe.loc[df_correspondant_index]['receiver_name']
                temp_paid_after_receiving = self.dataframe.loc[df_correspondant_index]['paid_after_receiving']
                temp_receiver_phone_nbr = self.dataframe.loc[df_correspondant_index]['receiver_phone_nbr']
                temp_receiver_mobile = self.dataframe.loc[df_correspondant_index]['receiver_mobile']
                temp_receiver_address = self.dataframe.loc[df_correspondant_index]['receiver_address']
                temp_content = self.dataframe.loc[df_correspondant_index]['content']
                temp_how_many = self.dataframe.loc[df_correspondant_index]['how_many']
                temp_how_much = self.dataframe.loc[df_correspondant_index]['how_much']
                temp_remark = self.dataframe.loc[df_correspondant_index]['remark']
                temp_shipping_id = self.dataframe.loc[df_correspondant_index]['shipping_id']
                temp_last_charged_date = self.dataframe.loc[df_correspondant_index]['last_charged_date']
                temp_charged = self.dataframe.loc[df_correspondant_index]['charged']
                temp_ifsend = self.dataframe.loc[df_correspondant_index]['ifsend']
                temp_ifcancel = self.dataframe.loc[df_correspondant_index]['ifcancel']
                temp_subcontent = self.dataframe.loc[df_correspondant_index]['subcontent']
                temp_shipping_link = self.dataframe.loc[df_correspondant_index]['shipping_link']

                print('write_in_2diff_db_2.2: Got all variables.')

                if not (temp_shipping_id == '' or pd.isnull(temp_shipping_id)):
                    _temp_logistic_company = None
                    if len(temp_shipping_id) == 10:
                        # 新竹物流的貨運編號長度為10，黑貓的長度為12
                        _temp_logistic_company = 'xinzhu'
                    elif len(temp_shipping_id) == 12:
                        _temp_logistic_company = 'black_cat'
                    
                    if _temp_logistic_company is not None:
                        temp_shipping_link = 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(temp_shipping_id) + '&logistic_company=' + _temp_logistic_company
                    else:
                        temp_shipping_link = self.dataframe.loc[df_correspondant_index]['shipping_link']
                
                # temp_unique_id = self.dataframe[self.dataframe['unique_id'] == ids]['unique_id'].tolist()[0]

                
                # print('新增訂單 : '+ ids)
                History_data.objects.create(unique_id = each_id, platform = temp_platform, 
                            file_created_date = temp_file_created_date,
                            txn_id = temp_txn_id  ,
                            customer_name = temp_customer_name,
                            receiver_name = temp_receiver_name,
                            paid_after_receiving  = temp_paid_after_receiving,
                            receiver_phone_nbr = temp_receiver_phone_nbr,
                            receiver_mobile  = temp_receiver_mobile,
                            receiver_address = temp_receiver_address,
                            content = temp_content ,
                            how_many = temp_how_many  ,
                            how_much  = temp_how_much,
                            remark = temp_remark,
                            shipping_id  = temp_shipping_id,
                            last_charged_date  = temp_last_charged_date,
                            charged = temp_charged,
                            ifsend  = temp_ifsend, 
                            ifcancel = temp_ifcancel,
                            subcontent  = temp_subcontent,
                            shipping_link = temp_shipping_link).save()
        

class HISTORY_DATA_db_writer:

    def __init__(self, **kwargs):   
        if 'dataframe_path' in kwargs.keys():
            self.dataframe = pd.read_excel(kwargs['dataframe_path'])
            print('model_tools', 'got df path.')
            # self.dataframe = 1
        elif 'dataframe' in kwargs.keys():
            self.dataframe = kwargs['dataframe']
            print('model_tools', 'got df.')
            # self.dataframe = 2
        else:
            raise 'ERROR! Has Not Input Dataframe.'

        self.column_names_dict = {
            '通路':'platform',
            '抓單日':'file_created_date',
            '訂單編號':'txn_id',
            '訂購人':'customer_name',
            '收件人':'receiver_name',
            '貨到付款':'paid_after_receiving',
            '電話':'receiver_phone_nbr',
            '手機':'receiver_mobile',
            '地址':'receiver_address',
            '內容物':'content',
            '數量':'how_many',
            '金額':'how_much',
            '備註':'remark',
            '宅單':'shipping_id',
            '最後回押日':'last_charged_date',
            '回押':'charged',
            '已寄出':'ifsend',
            '已取消':'ifcancel',
            '規格':'subcontent',
            '貨運連結':'shipping_link',
            'unique_id':'unique_id',
        }

    def _check_dataframe(self):
        # 確認該dataframe符合我們的格式
        #print(self.dataframe.columns)
        if 'unique_id' not in self.dataframe.columns:
            assert len(self.dataframe.columns) == 20
            assert sorted(list(self.dataframe.columns)) == sorted(list(self.column_names_dict.keys())[:-1])
            self.dataframe.loc[:, 'unique_id'] = self.dataframe['通路'] + '-' + self.dataframe['訂單編號'].astype(str)
        else:
            assert len(self.dataframe.columns) == 21
            assert sorted(list(self.dataframe.columns)) == sorted(list(self.column_names_dict.keys()))

        print('hddw1', 'check done.')


    def _make_dataframe_columns_to_match_db_columns(self):
        self.dataframe.columns = [self.column_names_dict[_] for _ in self.dataframe.columns]
        # 將DB Table的column name置換原本對應的中文column name
        
        for each_col in self.dataframe.columns:
            try:
                if each_col not in ['how_many','how_much']:
                    self.dataframe[each_col] = self.dataframe[each_col].apply(lambda x: x.strip())
            except:
                pass

        self.dataframe.file_created_date = \
            pd.to_datetime(self.dataframe.file_created_date).dt.date.astype(str)
        self.dataframe.last_charged_date = \
            self.dataframe.last_charged_date.apply(lambda x: '' if pd.isnull(x) else str(x))

        for _ in ['paid_after_receiving', 'ifsend', 'ifcancel', 'charged']:
            self.dataframe[_] = self.dataframe[_].apply(lambda x: False if (pd.isnull(x) or x == 'FALSE' or x == 'N' or x == 0 or x == False or x is None) else True)

        for _ in ['how_many','how_much']:
            self.dataframe[_] = self.dataframe[_].apply(lambda x: 0 if (pd.isnull(x) or x is None or x == '') else x)

        for _ in ['shipping_id']:
            self.dataframe[_] = self.dataframe[_].astype(str)

        print('hddw2 make_dataframe_columns_to_match_db_columns done.')


    def query_all_pending_txns(self):
        # 找出還未處理完(未取消也未出貨)的訂單, 並以pandas.dataframe形式回傳
        #pending_txns_objects = History_data.objects.raw('\
        #    select * from order_manage_History_data where ifsend = FALSE and ifcancel = FALSE;\')
        pending_txns_objects = History_data.objects.filter(ifsend=False, ifcancel=False).all()
        dataframe_tobe_returned = read_frame(pending_txns_objects).drop(['unique_id', 'id'], axis=1)

        mandarin_column_names = []
        for each_eng_column in dataframe_tobe_returned.columns:
            for k, v in self.column_names_dict.items():
                if v == each_eng_column:
                    mandarin_column_names.append(k)
                    break
        dataframe_tobe_returned.columns = mandarin_column_names

        return dataframe_tobe_returned
        

    def write_in_db(self):

        self._check_dataframe()
        self._make_dataframe_columns_to_match_db_columns()

        print(self.dataframe.head(1).T)
        print(self.dataframe.info())

        # 如果合併訂單的 uni_id跟資料庫裡的一樣，表示資料已存在
        # 則接著判斷寄出、取消狀態   aggregate的ifsend, ifcancel 會 = 0 而資料庫!= 0的話以資料庫為準
        for ids in self.dataframe['unique_id']: 
            if History_data.objects.filter(unique_id = ids):
                # 如果該unique_存在於該DB中
                #if self.dataframe[self.dataframe['unique_id'] == ids]['ifsend'].tolist()[0]):
                # 如果該筆交易已經寄出貨物
                # History_data.objects.filter(unique_id = ids) 的 ifsend
                History_data.objects.filter(unique_id = ids).update(ifsend = self.dataframe[self.dataframe['unique_id'] == ids]['ifsend'].tolist()[0])
                History_data.objects.filter(unique_id = ids).update(ifcancel = self.dataframe[self.dataframe['unique_id'] == ids]['ifcancel'].tolist()[0])
                _temp_shipping_id = self.dataframe[self.dataframe['unique_id'] == ids]['shipping_id'].tolist()
                _temp_logistic_company = None
                try:
                    _temp_shipping_id = self.dataframe[self.dataframe['unique_id'] == ids]['shipping_id'].tolist()[0]
                    print('write_in_db-1', _temp_shipping_id, type(_temp_shipping_id), len(_temp_shipping_id))
                    print('write_in_db-1.1', type(_temp_shipping_id))
                    if len(_temp_shipping_id) == 10:
                        # 新竹物流的貨運編號長度為10，黑貓的長度為12
                        _temp_logistic_company = 'xinzhu'
                    elif len(_temp_shipping_id) == 12:
                        _temp_logistic_company = 'black_cat'
                    print('write_in_db-1.2', _temp_logistic_company)
                except:
                    pass
                if _temp_logistic_company is not None:
                    print('write_in_db-2', _temp_logistic_company)
                    print(History_data.objects.filter(unique_id = ids).shipping_id)
                    History_data.objects.filter(unique_id = ids).update(shipping_id = _temp_shipping_id)
                    print(History_data.objects.filter(unique_id = ids).shipping_id)
                    History_data.objects.filter(unique_id = ids).update(shipping_link = 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(_temp_shipping_id) + '&logistic_company=' + _temp_logistic_company)
                print('已更新資料庫狀態:'+ ids )
            
            # 資料庫沒有這筆資料，要新增
            else:
                temp_platform = self.dataframe[self.dataframe['unique_id'] == ids]['platform'].tolist()[0]
                temp_file_created_date = self.dataframe[self.dataframe['unique_id'] == ids]['file_created_date'].tolist()[0]
                temp_txn_id = self.dataframe[self.dataframe['unique_id'] == ids]['txn_id'].tolist()[0]
                temp_customer_name = self.dataframe[self.dataframe['unique_id'] == ids]['customer_name'].tolist()[0]
                temp_receiver_name = self.dataframe[self.dataframe['unique_id'] == ids]['receiver_name'].tolist()[0]
                temp_paid_after_receiving = self.dataframe[self.dataframe['unique_id'] == ids]['paid_after_receiving'].tolist()[0]
                temp_receiver_phone_nbr = self.dataframe[self.dataframe['unique_id'] == ids]['receiver_phone_nbr'].tolist()[0]
                temp_receiver_mobile = self.dataframe[self.dataframe['unique_id'] == ids]['receiver_mobile'].tolist()[0]
                temp_receiver_address = self.dataframe[self.dataframe['unique_id'] == ids]['receiver_address'].tolist()[0]
                temp_content = self.dataframe[self.dataframe['unique_id'] == ids]['content'].tolist()[0]
                temp_how_many = self.dataframe[self.dataframe['unique_id'] == ids]['how_many'].tolist()[0]
                temp_how_much = self.dataframe[self.dataframe['unique_id'] == ids]['how_much'].tolist()[0]
                temp_remark = self.dataframe[self.dataframe['unique_id'] == ids]['remark'].tolist()[0]
                temp_shipping_id = self.dataframe[self.dataframe['unique_id'] == ids]['shipping_id'].tolist()[0]
                temp_last_charged_date = self.dataframe[self.dataframe['unique_id'] == ids]['last_charged_date'].tolist()[0]
                temp_charged = self.dataframe[self.dataframe['unique_id'] == ids]['charged'].tolist()[0]
                temp_ifsend = self.dataframe[self.dataframe['unique_id'] == ids]['ifsend'].tolist()[0]
                temp_ifcancel = self.dataframe[self.dataframe['unique_id'] == ids]['ifcancel'].tolist()[0]
                temp_subcontent = self.dataframe[self.dataframe['unique_id'] == ids]['subcontent'].tolist()[0]
                temp_shipping_link = self.dataframe[self.dataframe['unique_id'] == ids]['shipping_link'].tolist()[0]
                temp_unique_id = self.dataframe[self.dataframe['unique_id'] == ids]['unique_id'].tolist()[0]

                
                # print('新增訂單 : '+ ids)
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



if __name__ == '__main__':
    model_writer = HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=alicia.aggregated_txns)







