import pandas as pd, numpy as np
from .models import History_data
from django_pandas.io import read_frame
from .models import Subcontent_user_edit_record
pd.options.mode.chained_assignment = None
from django.db.models import Q
# from datetime import date as date_function

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
            '修訂出貨日':'edited_shipping_date',
            '最終出貨日':'final_shipping_date',
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
            '常溫宅單編號': 'room_temperature_shipping_id',
            '低溫宅單編號': 'low_temperature_shipping_id',
            '常溫貨運連結':'room_temperature_shipping_link',
            '低溫貨運連結':'low_temperature_shipping_link',
            '最後回押日':'last_charged_date',
            '回押':'charged',
            '已寄出':'ifsend',
            '已取消':'ifcancel',
            '供應商':'vendor',
            '規格':'subcontent',
            
            'unique_id':'unique_id',
        }

    def _check_if_has_value(self, target):
        if pd.isnull(target) or pd.isna(target) or target == '':
            return False
        else:
            return True

    def _check_dataframe(self):
        # 確認該dataframe符合我們的格式
        # print('_check_dataframe')
        # print('self.dataframe.columns\n', self.dataframe.columns)
        # print('self.dataframe.shape', self.dataframe.shape)
        # print(self.dataframe.head(1).T)
        try:
            if 'unique_id' not in self.dataframe.columns:
                # print('_check_dataframe: not having unique_id  >>', len(self.dataframe.columns))
                assert len(self.dataframe.columns) == 25
                dataframe_columns = list(self.dataframe.columns)
                columns_shoud_have = list(self.column_names_dict.keys())[:-1]
                assert all([_ in columns_shoud_have for _ in dataframe_columns]) == True
                self.dataframe.loc[:, 'unique_id'] = \
                    self.dataframe['通路'] + '|' + self.dataframe['供應商'] + '|' + self.dataframe['訂單編號'].astype(str)
                # print(self.dataframe.columns)
                # print(self.dataframe.loc[:, 'unique_id'])
            else:
                print('_check_dataframe: having unique_id  >>', len(self.dataframe.columns))
                assert len(self.dataframe.columns) == 26
                assert sorted(list(self.dataframe.columns)) == sorted(list(self.column_names_dict.keys()))
            self.dataframe.sort_values(by=['unique_id'], inplace=True)
            self.dataframe = self.dataframe.reset_index(drop=True)
        except Exception as e:
            print("Encountered Exception: ", e)
        # print('hddw1', 'check done.')
            

    def _make_dataframe_columns_to_match_db_columns(self):
        try:
            self.dataframe.columns = [self.column_names_dict[_] for _ in self.dataframe.columns]
            self.dataframe = self.dataframe[self.dataframe.file_created_date.notnull()]
            # print('hddw2 columns: ', self.dataframe.columns)
            self.dataframe.edited_shipping_date = self.dataframe.edited_shipping_date.apply(lambda x: None if x == '' else x)
            self.dataframe.final_shipping_date = self.dataframe.final_shipping_date.apply(lambda x: None if x == '' else x)
            # self.dataframe.unique_id = self.dataframe.unique_id.apply(lambda x: x.replace('\'', ''))
            # self.dataframe.txn_id = self.dataframe.txn_id.apply(lambda x: x.replace('\'', ''))
            self.dataframe.content = self.dataframe.content.apply(lambda x: x.replace('\'', ''))

            # 將DB Table的column name置換原本對應的中文column name
            for each_col in self.dataframe.columns:
                try:
                        self.dataframe[each_col] = self.dataframe[each_col].apply(lambda x: x.strip())
                except:
                    pass

            for _ in ['paid_after_receiving', 'ifsend', 'ifcancel', 'charged']:
                self.dataframe[_] = self.dataframe[_].apply(lambda x: False if (pd.isnull(x) or x == 'FALSE' or x == 'N' or x == 0 or x == False or x is None) else True)

            for _ in ['how_many','how_much']:
                self.dataframe[_] = self.dataframe[_].apply(lambda x: 0 if (pd.isnull(x) or x is None or x == '') else x)

            for _ in ['room_temperature_shipping_id', 'low_temperature_shipping_id']:
                self.dataframe[_] = self.dataframe[_].astype(str)

            # print('hddw2', 'make_dataframe_columns_to_match_db_columns done.')
        except Exception as e:
            print("hddw2 Exceeption: ", e)


    def query_all_pending_txns(self):
        # 找出還未處理完(未取消也未出貨)的訂單, 並以pandas.dataframe形式回傳
        #pending_txns_objects = History_data.objects.raw('\
        #    select * from order_manage_History_data where ifsend = FALSE and ifcancel = FALSE;\')
        pending_txns_objects = History_data.objects.filter(ifsend=False, ifcancel=False).order_by('final_shipping_date')
        dataframe_tobe_returned = read_frame(pending_txns_objects).drop(['unique_id', 'id'], axis=1)
        mandarin_column_names = []
        for each_eng_column in dataframe_tobe_returned.columns:
            for k, v in self.column_names_dict.items():
                if v == each_eng_column:
                    mandarin_column_names.append(k)
                    break
        dataframe_tobe_returned.columns = mandarin_column_names
        dataframe_tobe_returned = dataframe_tobe_returned[['通路', '抓單日', '修訂出貨日', '最終出貨日', 
        '訂單編號', '訂購人', '收件人', '貨到付款', '地址', '電話', '手機', '內容物', '金額', 
        '數量', '備註', '常溫宅單編號', '低溫宅單編號', '常溫貨運連結', '低溫貨運連結', '最後回押日', 
        '回押', '已寄出', '已取消', '供應商', '規格']]
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
        shipping_id = str(shipping_id).strip()
        if shipping_id is None or shipping_id == '':
            return ''
        else:
            _temp_logistic_company = None
            # 給定一個暫存的物流公司名稱
        try:
            if len(shipping_id) == 10:
                # 新竹物流的貨運編號長度為10，黑貓的長度為12
                _temp_logistic_company = 'xinzhu'
            elif len(shipping_id) == 12:
                _temp_logistic_company = 'black_cat'
        except Exception as e:
            # print('generate_shipping_link ERROR: ', e)
            return ''
        if _temp_logistic_company is not None:
            shipping_link = 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(shipping_id) + '&logistic_company=' + _temp_logistic_company
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
            txn_object.room_temperature_shipping_id = self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_id']
            txn_object.low_temperature_shipping_id = self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_id']
            txn_object.subcontent = self.dataframe.loc[df_correspondant_index]['subcontent']
            # 新增欄位，讓user可以修改的欄位增加
            # 其中一個原因是2020.08.04時曉箐反映有時客戶指定到貨日期時，
            # 她們會如此註記 周文斌 >> 周文斌(12/25到貨)
            # (曉箐原話:) 廠商不看備註!!他們都是直接看收件人的名字跟商品內容
            txn_object.customer_name = self.dataframe.loc[df_correspondant_index]['customer_name']
            txn_object.receiver_name = self.dataframe.loc[df_correspondant_index]['receiver_name']
            txn_object.content = self.dataframe.loc[df_correspondant_index]['content']
            txn_object.how_many = self.dataframe.loc[df_correspondant_index]['how_many']
            txn_object.file_created_date = self.dataframe.loc[df_correspondant_index]['edited_shipping_date']
            # txn_object.file_created_date = self.dataframe.loc[df_correspondant_index]['file_created_date']
            # print('History_data_update_1 Done: ', ids)
            # 在這裡寫上產出貨運連結的程式碼
            _temp_logistic_company = None
            
            txn_object.room_temperature_shipping_link = self.generate_shipping_link(
                self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_id']
            )

            txn_object.low_temperature_shipping_link = self.generate_shipping_link(
                self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_id']
            )

            txn_object.save()
   

        self._check_dataframe()
        self._make_dataframe_columns_to_match_db_columns()
        # print('write_in_2diff_db_1 Done')
        # print(self.dataframe.head(1).T)
        # print(self.dataframe.info())
        # print(self.dataframe.columns)
        #print('XX0', len(self.dataframe['unique_id'].tolist()))
        #print('XX1', self.dataframe['unique_id'].tolist())
        #print('self.dataframe[unique_id]', len(self.dataframe['unique_id']))
        #print('self.dataframe[unique_id]', self.dataframe['unique_id'])
        # 如果合併訂單的 uni_id跟資料庫裡的一樣，表示資料已存在
        # 則接著更新寄出、取消狀態

        for each_id in self.dataframe['unique_id'].tolist(): 
            # print('write_in_2diff_db_2', each_id)
            df_correspondant_index = self.dataframe[self.dataframe['unique_id']==each_id].index[0]
            # print('write_in_2diff_db_2 ', 'found df_correspondant_index')
            history_data_object = History_data.objects.filter(unique_id = each_id).first()
            # print('history_data_object is None: ', history_data_object is None)
            if history_data_object is not None:
                ## History_data 資料庫已有這筆資料

                # print('write_in_2diff_db_2.1: record(' + each_id + ') is in database.')
                if self._check_if_has_value(self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_link']):
                    _room_temperature_shipping_link = self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_link']
                else:
                    _room_temperature_shipping_link = self.generate_shipping_link(self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_id'])

                if self._check_if_has_value(self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_link']):
                    _low_temperature_shipping_link = self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_link']
                else:
                    _low_temperature_shipping_link = self.generate_shipping_link(self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_id'])


                # print('write_in_2diff_db_2.2: SHIPPING LINK  >>  ', _shipping_link)
                # print('write_in_2diff_db_2.3: df_correspondant_index  >>  ', df_correspondant_index)
                try:
                    for each_col in ['txn_id', 'customer_name', 'receiver_name', 'paid_after_receiving',
                    'receiver_address', 'receiver_phone_nbr', 'receiver_mobile', 'content', 'how_much', 
                    'how_many', 'remark', 'room_temperature_shipping_id', 'low_temperature_shipping_id', 
                    'last_charged_date', 'charged', 'vendor', 'ifsend', 'ifcancel', 'subcontent']:
                        setattr(history_data_object, each_col, self.dataframe.loc[df_correspondant_index][each_col])
                    
                    if self._check_if_has_value(self.dataframe.loc[df_correspondant_index]['edited_shipping_date']):
                        print(f"CHANGE DATE {self.dataframe.loc[df_correspondant_index]['edited_shipping_date']}")
                        setattr(history_data_object, 'edited_shipping_date', self.dataframe.loc[df_correspondant_index]['edited_shipping_date'])
                        setattr(history_data_object, 'final_shipping_date', self.dataframe.loc[df_correspondant_index]['edited_shipping_date'])

                    setattr(history_data_object, 'room_temperature_shipping_link', _room_temperature_shipping_link)
                    setattr(history_data_object, 'low_temperature_shipping_link', _low_temperature_shipping_link)

                    history_data_object.save()
                    # print('Done written in database.')

                except Exception as e:
                    print(f'encounter exception: {e}')
                # print('write_in_2diff_db_2.4: history_data_object has updated.')
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
            else:
                # 資料庫沒有這筆資料，要新增
                # print('write_in_2diff_db_2.1: not in db.')
                temp_platform = self.dataframe.loc[df_correspondant_index]['platform']
                temp_file_created_date = self.dataframe.loc[df_correspondant_index]['file_created_date']
                temp_file_edited_shipping_date = self.dataframe.loc[df_correspondant_index]['edited_shipping_date']
                if self._check_if_has_value(temp_file_edited_shipping_date):
                    temp_file_final_shipping_date = temp_file_edited_shipping_date
                else:
                    temp_file_final_shipping_date = temp_file_created_date
                # print('write_in_2diff_db_2.1: ', 'Done written dates.')
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
                temp_room_temperature_shipping_id = self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_id']
                temp_low_temperature_shipping_id = self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_id']
                temp_last_charged_date = self.dataframe.loc[df_correspondant_index]['last_charged_date']
                temp_charged = self.dataframe.loc[df_correspondant_index]['charged']
                temp_ifsend = self.dataframe.loc[df_correspondant_index]['ifsend']
                temp_ifcancel = self.dataframe.loc[df_correspondant_index]['ifcancel']
                temp_vendor = self.dataframe.loc[df_correspondant_index]['vendor']
                temp_subcontent = self.dataframe.loc[df_correspondant_index]['subcontent']
                # temp_room_temperature_shipping_link = self.dataframe.loc[df_correspondant_index]['room_temperature_shipping_link']
                # temp_low_temperature_shipping_link = self.dataframe.loc[df_correspondant_index]['low_temperature_shipping_link']
                #print('write_in_2diff_db_2.1: ', 'Done written others.')
                temp_room_temperature_shipping_link = self.generate_shipping_link(temp_room_temperature_shipping_id)
                temp_low_temperature_shipping_link = self.generate_shipping_link(temp_low_temperature_shipping_id)
                # print('write_in_2diff_db_2.2: Got all variables.')

                History_data.objects.create(
                    unique_id = each_id, 
                    platform = temp_platform, 
                    file_created_date = temp_file_created_date,
                    edited_shipping_date = None if temp_file_edited_shipping_date == '' or pd.isnull(temp_file_edited_shipping_date) else temp_file_edited_shipping_date,
                    final_shipping_date = temp_file_final_shipping_date,
                    txn_id = temp_txn_id,
                    customer_name = temp_customer_name,
                    receiver_name = temp_receiver_name,
                    paid_after_receiving  = temp_paid_after_receiving,
                    receiver_phone_nbr = temp_receiver_phone_nbr,
                    receiver_mobile  = temp_receiver_mobile,
                    receiver_address = temp_receiver_address,
                    content = temp_content,
                    how_many = temp_how_many ,
                    how_much  = temp_how_much,
                    remark = temp_remark,
                    room_temperature_shipping_id = temp_room_temperature_shipping_id,
                    low_temperature_shipping_id = temp_low_temperature_shipping_id,
                    last_charged_date  = temp_last_charged_date,
                    charged = temp_charged,
                    ifsend  = temp_ifsend, 
                    ifcancel = temp_ifcancel,
                    vendor = temp_vendor,
                    subcontent  = temp_subcontent,
                    room_temperature_shipping_link = temp_room_temperature_shipping_link,
                    low_temperature_shipping_link = temp_low_temperature_shipping_link,
                    ).save()
                # print('Done 新增訂單 : '+ each_id)
                #except Exception as e:
                #    print("EXCEPTION: ", e)
                #    break
        
        # 進行 將 2021.01.01後的 官網年菜 訂單歸一的行為
        #   只要是相同的 訂單編號，假設裡面有a, b, c, d 四筆訂單，
        #   如果任何一筆含有「青葉臺菜」，就把這四筆歸為同一筆訂單。
    
    def qingye_cleaning(self):
    
        qingye_after_210101_official_queryset = \
            History_data.objects.filter(file_created_date__gte = '2021-01-01', platform = '樂天派官網')

        unique_ids_with_qingyetaicai = \
            list(set(qingye_after_210101_official_queryset.values_list('txn_id', flat=True).filter(
                Q(content__contains='青葉臺菜') | Q(content__contains='青葉台菜')
            )))

        ids_with_qingyetaicai = \
            list(qingye_after_210101_official_queryset.values_list('txn_id', flat=True).filter(txn_id__in=unique_ids_with_qingyetaicai))

        #print(qingye_after_210101_official_queryset.values().filter(
        #        Q(content__contains='青葉臺菜') | Q(content__contains='青葉台菜')
        #    ))
        unique_ids_with_qingyetaicai = list(set(ids_with_qingyetaicai))

        if not len(ids_with_qingyetaicai) == len(unique_ids_with_qingyetaicai):
            print(f'unique_ids_with_qingyetaicai != ids_with_qingyetaicai >> \
                {len(unique_ids_with_qingyetaicai)} != {len(ids_with_qingyetaicai)}')

            count = 0
            for each_txn_id in unique_ids_with_qingyetaicai:
                sub_queryset = qingye_after_210101_official_queryset.filter(txn_id=each_txn_id)
                _final_content = list()
                if sub_queryset.count() > 1:
                    count += 1   
                    # 代表有應該合併的訂單存在
                    the_ids = [_.id for _ in sub_queryset]
                    try:
                        for _content, _how_many in zip([_.content for _ in sub_queryset], [_.how_many for _ in sub_queryset]):
                            #print(f'_final_content {_final_content}')
                            #print(_content, _how_many)
                            _final_content.append(
                                _content + '*' + str(_how_many))
                        _final_content = ', '.join(_final_content)
                    except Exception as e:
                        print(f'in_clean_1_except: {e}')
                    try:        
                        the_qingye_one_object = sub_queryset.filter(
                            Q(content__contains='青葉臺菜') | Q(content__contains='青葉台菜')).first()
                        
                        History_data.objects.create(
                            unique_id = the_qingye_one_object.unique_id, 
                            platform = '樂天派官網', 
                            file_created_date = the_qingye_one_object.file_created_date,
                            edited_shipping_date = the_qingye_one_object.edited_shipping_date,
                            final_shipping_date = the_qingye_one_object.final_shipping_date,
                            txn_id = the_qingye_one_object.txn_id,
                            customer_name = the_qingye_one_object.customer_name,
                            receiver_name = the_qingye_one_object.receiver_name,
                            paid_after_receiving  = the_qingye_one_object.paid_after_receiving,
                            receiver_phone_nbr = the_qingye_one_object.receiver_phone_nbr,
                            receiver_mobile  = the_qingye_one_object.receiver_mobile,
                            receiver_address = the_qingye_one_object.receiver_address,
                            content = _final_content,
                            how_many = 1 ,
                            how_much  = sum([_.how_much for _ in sub_queryset]),
                            remark = the_qingye_one_object.remark,
                            room_temperature_shipping_id = the_qingye_one_object.room_temperature_shipping_id,
                            low_temperature_shipping_id = the_qingye_one_object.low_temperature_shipping_id,
                            last_charged_date  = the_qingye_one_object.last_charged_date,
                            charged = the_qingye_one_object.charged,
                            ifsend  = the_qingye_one_object.ifsend, 
                            ifcancel = the_qingye_one_object.ifcancel,
                            vendor = '青葉',
                            subcontent  = ', '.join(_.subcontent for _ in sub_queryset),
                            room_temperature_shipping_link = the_qingye_one_object.room_temperature_shipping_link,
                            low_temperature_shipping_link = the_qingye_one_object.low_temperature_shipping_link
                        ).save()
                        # print(each_txn_id_with_qingye)
                        qingye_after_210101_official_queryset.filter(id__in=the_ids).delete()
                    except Exception as e:
                        print(f'in_clean_2_except: {e}')

            print(f'Dealt with {count} records.')

        else:
            print(f'unique_ids_with_qingyetaicai == ids_with_qingyetaicai >> \
                {len(unique_ids_with_qingyetaicai)} == {len(ids_with_qingyetaicai)}')
        
        '''for each_txn_id_with_qingye in txn_ids_with_qingyetaicai:
            sub_queryset = qingye_after_210101_official_queryset.filter(txn_id__in=each_txn_id_with_qingye)
            # 內容物的部分需要與 數量結合在一起
            _final_content = list()
            for _content, _how_many in zip(list(sub_queryset.values_list('content', flat=True)), list(sub_queryset.values_list('how_many', flat=True))):
                _final_content.append(
                    _content + '*' + str(_how_many)
                )
            _final_content = ', '.join(_final_content)
            if sub_queryset.count() > 1:
                # 代表有應該合併的訂單存在
                the_qingye_one_object = sub_queryset.filter(content__contains='青葉臺菜').first()
                History_data.objects.create(
                    unique_id = the_qingye_one_object.unique_id, 
                    platform = '樂天派官網', 
                    file_created_date = the_qingye_one_object.file_created_date,
                    edited_shipping_date = the_qingye_one_object.edited_shipping_date,
                    final_shipping_date = the_qingye_one_object.final_shipping_date,
                    txn_id = the_qingye_one_object.txn_id,
                    customer_name = the_qingye_one_object.customer_name,
                    receiver_name = the_qingye_one_object.receiver_name,
                    paid_after_receiving  = the_qingye_one_object.paid_after_receiving,
                    receiver_phone_nbr = the_qingye_one_object.receiver_phone_nbr,
                    receiver_mobile  = the_qingye_one_object.receiver_mobile,
                    receiver_address = the_qingye_one_object.receiver_address,
                    content = _final_content,
                    how_many = 1 ,
                    how_much  = sum(sub_queryset.values_list('how_much', flat=True)),
                    remark = the_qingye_one_object.remark,
                    room_temperature_shipping_id = the_qingye_one_object.room_temperature_shipping_id,
                    low_temperature_shipping_id = the_qingye_one_object.low_temperature_shipping_id,
                    last_charged_date  = the_qingye_one_object.last_charged_date,
                    charged = the_qingye_one_object.charged,
                    ifsend  = the_qingye_one_object.ifsend, 
                    ifcancel = the_qingye_one_object.ifcancel,
                    vendor = '青葉',
                    subcontent  = ', '.join(list(the_qingye_one_object.values_list('subcontent', flat=True))),
                    room_temperature_shipping_link = the_qingye_one_object.room_temperature_shipping_link,
                    low_temperature_shipping_link = the_qingye_one_object.low_temperature_shipping_link
                ).save()
                print(each_txn_id_with_qingye)
                sub_queryset.delete()'''

        

if __name__ == '__main__':
    model_writer = HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=alicia.aggregated_txns)







