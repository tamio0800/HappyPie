# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 22:51:04 2020
"""
import sys
import re
import os
import pandas as pd
import time
import numpy as np
import msoffcrypto as ms
import ntpath
import xlrd
from datetime import datetime, timedelta
import pyDes
import base64


class ALICIA:

    def __init__(self):
        self.data_path = os.path.join(os.path.dirname(__file__),'data')
        self.platforms = self._get_platforms()    # 共有哪一些平台的資料需要整合
        self.platform_name_rules = self._build_platform_name_patterns()    # 讀取分析各平台報告的檔案名稱規則
        self.aggregated_txns_columns = self._get_aggregated_txns_columns()    # 整合報表包含哪些欄位
        self.raw_txns_dir = os.path.join(os.path.abspath(os.curdir), 'order_manage/ALICIA/decrypt')  # 存放『從各平台下載的報表』的資料夾路徑
        # self.decr_raw_txns_dir = os.path.join(os.path.abspath(os.curdir), 'order_manage/ALICIA/decrypt')  # 存放『需要密碼的』已解密的報表的資料夾路徑 
        self.aggregated_txns = self._build_aggregated_txns_table()    # 存放整合交易的DATAFRAME
        self.user_uploaded_aggregated_txns = self._build_aggregated_txns_table()    # 存放user主動上傳整合交易檔的DATAFRAME
        self.unique_aggregated_txns = self._build_aggregated_txns_table()    # 存放整合交易的TABLE
        self.log_content = []    # 存放使用紀錄, 目前(2020.04.02)還沒想到要怎麼做
        self.password_dict = self._get_passwords()  # 回傳密碼集
        

    def check_if_all_files_are_good_for_ALICIA_pipeline(self, file_list):
        # 在這個函式當中, 我們將檢查【檔案命名】以及其【附檔名】是否皆符合我們的要求
        # 檔案命名指的, 其是否meet一種我們的平台交易資料的正則表達式情境

        def check_if_all_are_excel_files(file_list):
            exception_files = []
            criteria = ('xlsx', 'xlsm', 'xls', 'csv', 'xlsb')
            meet_criteria = 0
            for each_file in file_list:
                if each_file.endswith(criteria):
                    meet_criteria += 1
                    # 加總符合條件的檔案, 接著拿這個數字比對檔案數量就知道是不是都符合條件了
                else:
                    exception_files.append(each_file)
            return (meet_criteria == len(file_list), sorted(exception_files))

        def check_if_each_file_meets_1_regex_condition(file_list):
            exception_files = []
            for each_file in file_list:
                is_meet_criteria = 0
                for each_re_rule in self.platform_name_rules.values():
                    if re.search(each_re_rule, each_file) is not None:
                        # meet到特定的平台命名規則了
                        is_meet_criteria = 1
                        break
                if not is_meet_criteria:
                    exception_files.append(each_file)  
            return (len(exception_files)==0, sorted(exception_files))
        
        return_excel_check, exceptions_excel_check = \
            check_if_all_are_excel_files(file_list)
        
        return_regex_check, exceptions_regex_check = \
            check_if_each_file_meets_1_regex_condition(file_list)
        
        return  (return_excel_check+return_regex_check==2,
                 sorted(list(set(exceptions_excel_check+exceptions_regex_check))))
    
    def check_if_the_xl_file_is_encrypted(self, file):
        try:
            # 如果沒有加密的話就能直接打開
            xlrd.open_workbook(file)
            # print(file, 'is NOT encrypted.')
            return False
        except:
            # print(file, 'is encrypted.')
            return True

    def move_files_and_decrypt_them(self, from_dir, to_dir):
        # 先移動需要密碼的那三個平台, 因為處理方式略為不同
        assert from_dir != to_dir
        for encrypted_platform in ['MOMO', '亞伯', '東森得易購']:
            encrypted_txn_files = self._return_txn_path(from_dir, encrypted_platform)
            for each_encrypted_txn_file in encrypted_txn_files:
                # 下面這句是將路徑/檔案名 分解成 路徑, 檔案名, 使用ntpath在linux與windows環境下都可以正常運作
                _, tail_of_file = ntpath.split(each_encrypted_txn_file)
                if self.check_if_the_xl_file_is_encrypted(each_encrypted_txn_file):
                    the_file = ms.OfficeFile(open(each_encrypted_txn_file, 'rb'))
                    the_file.load_key(password=self.password_dict[encrypted_platform])
                    the_file.decrypt(open(os.path.join(to_dir, tail_of_file), 'wb'))
                    os.unlink(each_encrypted_txn_file)
                    # print('Successfully Moved ', tail_of_file)
        # 把剩下的檔案移一移, 包括原先就沒有加密的跟理論上會加密但沒有加密的那些檔案
        for each_file in os.listdir(from_dir):
            os.rename(
                os.path.join(from_dir, each_file),
                os.path.join(to_dir, each_file)
            )
            # print('Successfully Moved ', tail_of_file)

    def delete_files_in_the_folder(self, folder_path):
        for each_file in os.listdir(folder_path):
            # 盡可能刪除裡面所有的檔案
            try:
                if not each_file.endswith('fortracked'):
                    os.unlink(os.path.join(folder_path, each_file))
                    print(each_file)
            except:
                pass

    def pre_clean_raw_txns(self):
        if self.aggregated_txns.shape[0] > 0:
            # self.aggregated_txns 至少要有東西再清理

            # 以通路 + 編號 + 內容物作為暫時的unique_id,
            # 來作為A交易在昨天與今天一起被重複匯進來的處理機制
            self.aggregated_txns.loc[:, 'pre_clean_unique_id'] = self.aggregated_txns['通路'] + '-' + \
                self.aggregated_txns['訂單編號'].astype(str) + '-' + \
                self.aggregated_txns['內容物']
            self.aggregated_txns = self.aggregated_txns.drop_duplicates(subset='pre_clean_unique_id', keep='first')
            self.aggregated_txns = self.aggregated_txns.sort_values('pre_clean_unique_id').reset_index(drop=True)
            self.aggregated_txns = self.aggregated_txns.drop(['pre_clean_unique_id'], axis=1)

            self.aggregated_txns.loc[:, 'unique_id'] = self.aggregated_txns['通路'] + '-' + \
                self.aggregated_txns['訂單編號'].astype(str)
        
            # 針對亞伯做特殊處理
            yabo_part = self.aggregated_txns[self.aggregated_txns['通路']=='亞伯']
            non_yabo_part = self.aggregated_txns[~self.aggregated_txns.index.isin(yabo_part.index)]
            if yabo_part.shape[0] > 0:
                print('pre_clean_raw_txns 2:  Found Yabo!')
                #yabo_part.to_excel('pre_clean_raw_txns2.1_yabo_part.xlsx', index=False)
                _temp_df = pd.DataFrame(columns=yabo_part.columns)
                yabo_part.loc[:, 'unique_id'] = yabo_part['unique_id'].apply(lambda x: '-'.join(x.split('-')[:-1]))
                # print('pre_clean_raw_txns 2.1: ', yabo_part.loc[:, 'unique_id'].unique().tolist())
                # 將unique_id去掉(會員訂單編號的部分)，為了將同一筆訂單合併
                for each_unique_id in yabo_part['unique_id'].unique().tolist():
                    tdf_yabo_part = yabo_part[yabo_part['unique_id'] == each_unique_id]
                    # print('pre_clean_raw_txns 2.1.1: ', tdf_yabo_part['規格'].unique().tolist())
                    for each_unique_subcontent_under_the_id in tdf_yabo_part['規格'].unique().tolist():
                       # 開始將內容填入_temp_df中
                        _temp_df.loc[_temp_df.shape[0]] = \
                            tdf_yabo_part[tdf_yabo_part['規格']==each_unique_subcontent_under_the_id].head(1).values[0]
                        # 先拿第一排
                        _temp_df.loc[_temp_df.shape[0] - 1, '數量'] = tdf_yabo_part[tdf_yabo_part['規格']==each_unique_subcontent_under_the_id]['數量'].astype(int).sum()
                        # 計算數量加總
                print('pre_clean_raw_txns 2.1: ', _temp_df.shape)
                #_temp_df.to_excel('pre_clean_raw_txns2.2_temp_df.xlsx', index=False)
                self.aggregated_txns = pd.concat([non_yabo_part, _temp_df])  # 將兩者合併


    def get_today(self, format='%Y%m%d'):
        return datetime.today().strftime(format)

    def remove_unique_id(self):
        if 'unique_id' in self.aggregated_txns.columns:
            self.aggregated_txns = self.aggregated_txns.drop(['unique_id'], axis=1)

    def wait_till_the_flag_comes_up(self, path_of_the_flag_to_wait, path_of_new_flag, sleep_seconds=2):
        is_it_good_to_do_something = 0
        while(is_it_good_to_do_something == 0):
            if os.path.isfile(path_of_the_flag_to_wait):
                is_it_good_to_do_something = 1
            else:
                time.sleep(sleep_seconds)
        os.unlink(path_of_the_flag_to_wait)
        with open(path_of_new_flag, 'w') as w:
            w.write('done!')

    def combine_aggregated_txns_and_user_uploaded_aggregated_txns(self, not_user_uploaded_df, user_uploaded_df):
        def clean_number_like_columns(df):
            df['訂單編號'] = df['訂單編號'].apply(self.try_to_be_int_in_str)
            df['宅單'] = df['宅單'].apply(lambda x: re.sub(re.compile(r'[- －]'), '', str(x))).apply(self.try_to_be_int_in_str)
            df['手機'] = df['手機'].apply(self.make_phone_and_mobile_number_clean)
            # df['電話'] = df['電話'].apply(self.make_phone_and_mobile_number_clean)
            return df

        if not_user_uploaded_df is not None:
            if user_uploaded_df is not None:
                not_user_uploaded_df.loc[:, 'unique_id'] = \
                    not_user_uploaded_df['通路'] + '-' + not_user_uploaded_df['訂單編號'].astype(str)

                user_uploaded_df.loc[:, 'unique_id'] = \
                    user_uploaded_df['通路'] + '-' + user_uploaded_df['訂單編號'].astype(str)

                # 接著要整理一下，如果user_uploaded_df裡有的交易，就從not_user_uploaded_df中刪除
                not_user_uploaded_df = \
                    not_user_uploaded_df[~not_user_uploaded_df.unique_id.isin(user_uploaded_df.unique_id)]

                # 再增加一個條件以加速資料寫入的流程>> 1個月以前的交易不做更新(直接從這次的batch中排除)
                not_user_uploaded_df = not_user_uploaded_df[
                    pd.to_datetime(not_user_uploaded_df['抓單日']) > (pd.to_datetime(not_user_uploaded_df['抓單日'])  - pd.Timedelta(days=31))
                ]
                
                _temp_df = pd.concat([not_user_uploaded_df, user_uploaded_df], join='inner').reset_index(drop=True)
                return clean_number_like_columns(_temp_df)
            else:
                return clean_number_like_columns(not_user_uploaded_df)
        else:
            if user_uploaded_df is not None:
                return clean_number_like_columns(user_uploaded_df)
            else:
                return None

    
    def to_one_unique_id_df_after_kash(self, dataframe_with_unique_id_column, linked_symbol=',\n'):
        # unique_id 理想上由  通路-訂單編號  三個元素構成
        # 此函式是為了將含有多個"unique_id"的dataframe整合成真正的unique_id
        # linked_symbol是連接符號, 用以連接複數"unique_id"的內容物們, 如:
        # 0003: 冰淇淋任選2件/香草口味
        # 0003: 冰淇淋任選2件/草莓口味
        # >> 0003: 冰淇淋任選2件/香草口味【連接符號】冰淇淋任選2件/草莓口味
        
        assert dataframe_with_unique_id_column.shape[0] > 0
        # dataframe至少要有東西再丟進來清理

        if dataframe_with_unique_id_column.shape[0] != len(dataframe_with_unique_id_column.unique_id.unique()):
            # dataframe 長度與 其中的 unique_id 長度不同, 代表需要進行整合歸戶(unique_id)
            _temp_df = dataframe_with_unique_id_column[dataframe_with_unique_id_column.unique_id.apply(
                lambda x: True if dataframe_with_unique_id_column[dataframe_with_unique_id_column.unique_id==x].shape[0] == 1 else False)].reset_index(drop=True)
            _multi_df = dataframe_with_unique_id_column[~dataframe_with_unique_id_column.unique_id.isin(_temp_df.unique_id)].reset_index(drop=True)
            # 將只有1個row的交易移到_temp_df中, 剩下的放到_multi_df中

            for each_unique_txn_id in _multi_df.unique_id.unique():
                _temp_small_multi_df = _multi_df[_multi_df.unique_id==each_unique_txn_id].reset_index(drop=True)
                # 創建一個對應的dataframe, 內含交易為對應的unique_txn_id
                _temp_small_multi_df.loc[0, '內容物'] = self._combine_columns(
                        _temp_small_multi_df['內容物'].tolist(), linked_symbol)
                _temp_small_multi_df.loc[0, '備註'] = self._combine_columns(
                        _temp_small_multi_df['備註'].tolist(), linked_symbol)
                _temp_small_multi_df.loc[0, '規格'] = self._combine_columns(
                        _temp_small_multi_df['規格'].tolist(), ', ')
                try:
                    _temp_small_multi_df.loc[0, '金額'] = _temp_small_multi_df['金額'].astype(int).sum()
                except:
                    pass
                
                _temp_df.loc[_temp_df.shape[0]] = \
                        _temp_small_multi_df.loc[0].tolist()
            return _temp_df
        return dataframe_with_unique_id_column
                

    def _build_platform_name_patterns(self):
        # 建立各個平台文件的名稱規則, 以利於分辨每一份報表分屬於哪一些平台
        _temp = {'好吃市集': re.compile(r'^2[0-9]{3}-[0-9]{2}-[0-9]{2}_好吃市集_\S+'),
                 '生活市集': re.compile(r'^2[0-9]{3}-[0-9]{2}-[0-9]{2}_生活市集_\S+'),
                 '樂天派官網': re.compile(r'.*export_[0-9]{2}\w{3}[0-9]{2}\s{0,2}.*xls[x]{0,1}$|.*2[0-9]{7}_export_default.xls[x]{0,1}'),
                 'MOMO': re.compile(r'[A-Z]\d+_\d_\d+_\d+_[20]\d+.xls|\S+\d+\s{0,2}[(]MOMO[)].xls|.*訂單查詢-第三方物流.*xls[x]{0,1}$|[A-Z]\d+_\d_\d+_[20]\d+.xls'),
                 '亞伯': re.compile(r'[a-z]\d+_PoDetail_\d+.xls|\S+PoDetail_\d+\s{0,2}[(]亞伯[)].xls|[a-z]\d+_shipmentReport_\d+.xls'),
                 '東森得易購': re.compile(r'^[a-z0-9]{8}_20\d+.xls'),
                 'Yahoo購物中心': re.compile(r'^delivery - [0-9]{4}-[0-9]{2}-[0-9]{2}\S+\s{0,2}[(]YAHOO購物中心[)].xls|^delivery - [0-9]{4}-[0-9]{2}-[0-9]{2}\S+\s{0,2}.xls'),
                 'UDN': re.compile(r'^Order_2[0-9]{16}[(][Uu][Dd][Nn][)]'),
                 'Friday': re.compile(r'^OrderData_[0-9]{5} - 2[0-9]{3}-[0-9]{2}-\S+.csv'),
                 '博客來': re.compile(r'^take_order_2[0-9]{13}\s{0,2}[(]博客來[)].xls|^take_order_2[0-9]{13}\s{0,2}.xls'),
                 '台塑': re.compile(r'^Order_2[0-9]{16}[(]台塑[)]'),
                 '整合檔': re.compile(r'.*20[0-9]{6}-[0-9]{6}_.*整合檔.*.xls[x]{0,1}'),
                 'LaNew': re.compile(r'.*_\w{5}_2[0-9]{3}[01][0-9][0123][0-9].xls[x]{0,1}'),
                 '快車肉乾銷港': re.compile(r'.{0,6}orders\s*[(]{0,1}\d*[)]{0,1}\s*.csv|.{0,6}orders\s*[(]{0,1}\d*[)]{0,1}\s*.xls[x]{0,1}'),
                }
        # 我們把 "整合檔" 也當作一個平台來處理，只是它不需要被再度整合、也不需要丟進kashgari做分析
        # by Annie and Tamio @2020.06.24
        return _temp


    def _get_passwords(self):
        # 這樣寫以便更容易的管理個平台密碼
        _temp_dict = {}
        with open(os.path.join(self.data_path, 'passwords.txt'), 'r') as r:
            for _ in r:
                platform_name, platform_password = _.split(':')
                _temp_dict[platform_name.strip()] = platform_password.strip()
        return _temp_dict


    def _return_txn_path(self, target_dir, platform_name):
        assert platform_name in list(self.platform_name_rules.keys())
        # 檢查要查找的平台名稱是否存在於各個平台文件的名稱規則字典中
        _temp_paths = []

        for _file in os.listdir(target_dir):
            if re.match(self.platform_name_rules[platform_name], _file) is not None:
                # 有找到匹配的檔案名稱
                _temp_paths.append(os.path.abspath(os.path.join(target_dir, _file)))
        return _temp_paths


    def _is_int(self, target):
        # 檢查是否輸入值為整數
        try:
            if int(target) == float(target):
                return True
            else:
                return False
        except:
            return False

    def _get_platforms(self):
        # 這樣寫是為了更好的管理平台名稱
        _temp = []
        with open(os.path.join(self.data_path, 'platforms.txt'), 'r', errors='ignore') as r:
            _temp.extend([_.strip() for _ in r])
        return _temp


    def _get_aggregated_txns_columns(self):
        # 這樣寫是為了更好的管理整合報表的欄位名稱
        _temp = []
        with open(os.path.join(self.data_path, 'aggregated_txns_columns.txt'), 'r', errors='ignore') as r:
            _temp.extend([_.strip() for _ in r])
        return _temp


    def _build_aggregated_txns_table(self):
        # 產製整合交易資料(原始訂單)的框架
        return pd.DataFrame(columns=self.aggregated_txns_columns)


    def _clean_dataframe(self, pandas_dataframe, strip_only=False, make_null_be_nullstring=False, **kwargs):
        assert type(pandas_dataframe) is pd.core.frame.DataFrame
        columns_cannot_be_ffill = [
            '載具編號', '備註', '轉帳帳號', '室內電話', '預計配送日', '買家備註', '賣家備註',
            '購物車備註', '商品屬性', '活動序號', '配送備註', '購買備註', '特標語', 'shipxrem', 'xrem', 'spslgn',
            '指交日期', 'sstockdat', '訂單備註', '搭配活動', '退/換貨原因', '宅單備註', '配送訊息', '約定配送日'
            ]
        # 檢查輸入值是否為pandas dataframe CLASS
        for each_col in pandas_dataframe.columns:
            print('ALICIA _clean_dataframe 1: ', each_col)
            if not strip_only:
                if each_col not in columns_cannot_be_ffill:
                    pandas_dataframe.loc[:, each_col] = pandas_dataframe.loc[:, each_col].fillna(method='ffill')
                    # 為了處理merged cell會為null值的問題,
                    # 將每個row由上往下最後一次看到的非null值分配給下方的null值
            else:
                try:
                    pandas_dataframe.loc[:, each_col] = pandas_dataframe.loc[:, each_col].apply(lambda x: x.strip() if not pd.isnull(x) else x)
                except:
                    pass

            if make_null_be_nullstring:
                pandas_dataframe[each_col][pd.isnull(pandas_dataframe[each_col])] = ''
                pandas_dataframe[each_col] = pandas_dataframe[each_col].apply(lambda x: '' if x in ['nan', 'null', 'NULL', 'None', 'none', 'NONE'] else x)
            
            try:
                pandas_dataframe.loc[:, each_col] = pandas_dataframe.loc[:, each_col].apply(lambda x: x if not pd.isnull(x) else x)
            except Exception as e:
                print(e)
                pass
            
        # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        # 我想在這個函式裡加上一個功能，讓這個函式能夠處理如：將False轉成N，或null值轉成Flase等等的任務，       
        # 主要是讓user在看報表時，可以不要讓【是否取消】或【已出貨】的欄位內容充滿TRUE/FALSE那樣凌亂，         
        # 原先是想寫在上面那個迴圈，但擔心程式碼變太亂反而之後修改不易，而且多一個迴圈應該也不會消耗太多運算資源，
        # 所以就來新建一個迴圈專門幹這件事吧!
        # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        # 新建一個**kwargs參數，如果有要進行方便user閱讀處理的話就必須要同時輸入 >> 
        # 【easy_read_for_users=True】 & 【dealing_columns=['column1', 'column2'...]】；
        # 若今天是收取user上傳的檔案時，我們就需要反向而行，將簡化後的資料轉成符合我們資料庫型態的格式，此時需要 >>
        # 【to_database_format=True】 & 【dealing_columns=['column1', 'column2'...]】。
        # by Tamio @2020.06.25
        # = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

        if 'dealing_columns' in kwargs.keys():
            # 有columns需要額外進行處理
            if 'easy_read_for_users' in kwargs.keys() and kwargs['easy_read_for_users'] == True:
                # 將資料轉成user方便容易閱讀的格式
                # 我的想法是正向表列，唯有TRUE才顯示為Y，其餘的都顯示空白就可以了
                def easy_to_read(target_string):
                    try:
                        if target_string is None:
                            return ''
                        elif target_string in ['TRUE', True, 'true']:
                            return 'Y'
                        else:
                            return ''
                    except:
                        return ''
                for each_col in kwargs['dealing_columns']:
                    try:
                        pandas_dataframe[each_col] = pandas_dataframe[each_col].apply(easy_to_read)
                    except Exception as e:
                        print(e)
            
            if 'to_database_format' in kwargs.keys() and kwargs['to_database_format'] == True:
                def to_db_format(target_string):
                    try:
                        if target_string is None:
                            return False
                        elif target_string in [True, 'TRUE', 'true', 'yes', 'YES', 'Y', 'y', 'O']:
                            # 正面表列, 因為大多數情況這個應該都不為真
                            return True
                        else:
                            return False
                    except:
                        return False
                for each_col in kwargs['dealing_columns']:
                    try:
                        pandas_dataframe[each_col] = pandas_dataframe[each_col].apply(to_db_format)
                    except Exception as e:
                        print(e)

        return pandas_dataframe


    def _combine_columns(self, combine_1_dim_array, linked, only_meaningful=False):
        _temp = ''
        if not only_meaningful:
            for _, _element in enumerate(combine_1_dim_array):
                _element = str(_element).strip()
                if not (pd.isnull(_element) or _element == '' or _element == '共同'):
                    if _ == 0:
                        _temp += str(_element)
                    else:
                        _temp += linked + str(_element)
        else:
            # 只回傳有意義的部份回去就好
            # 先看看後面的元素有沒有包含重要資訊
            # 以東森得易購為例(先開發它就好)，只要不是 "共同"或空白都是有意義的
            for _, _element in enumerate(combine_1_dim_array[1:]):
                _element = str(_element).strip()
                if not (pd.isnull(_element) or _element == '' or _element == '共同'):
                    if _ == 0:
                        _temp += str(_element)
                    else:
                        _temp += linked + str(_element)
            if len(_temp) == 0:
                # 沒看到什麼重要的資訊
                _temp = combine_1_dim_array[0].strip()
        return _temp    


    def _get_file_created_date(self, file_path):
        return time.strftime('%Y-%m-%d', time.gmtime(os.path.getmtime(file_path)))

    
    def _get_unique_txns(self):
        if self.aggregated_txns.shape[0]:
            # 上面那行代表 aggregated_txns dataframe裡面有資料
            self.aggregated_txns.loc[:, 'temp_unique_id'] = self.aggregated_txns['抓單日'] + '-' + \
                                                       self.aggregated_txns['訂單編號'].astype(str) + '-' + \
                                                       self.aggregated_txns['通路']
                                                            
            #df.loc[:, 'temp_unique_id'] = df['抓單日'] + '-' + \
            #                                                df['訂單編號'].astype(str) + '-' + \
            #                                                df['通路']                                             
            # 創立一個暫時的unique_id
            if self.aggregated_txns.shape[0] != len(self.aggregated_txns['temp_unique_id'].unique()):
                # dataframe 長度與 其中的 unique_id 長度不同, 代表需要進行整合歸戶(id)
                _temp_df = self.aggregated_txns[self.aggregated_txns.temp_unique_id.apply(
                        lambda x: True if self.aggregated_txns[self.aggregated_txns.temp_unique_id==x].shape[0] == 1 else False)].reset_index(drop=True)
                _multi_df = self.aggregated_txns[~self.aggregated_txns.temp_unique_id.isin(_temp_df.temp_unique_id)].reset_index(drop=True)
                
                # 將只有1個row的交易移到_temp_df中, 剩下的放到_multi_df中

                #temp_df =df[df.temp_unique_id.apply(
                #        lambda x: True if df[df.temp_unique_id==x].shape[0] == 1 else False)].reset_index(drop=True)
                #multi_df = df[~df.temp_unique_id.isin(temp_df.temp_unique_id)] 
                #mdf = multi_df[multi_df.temp_unique_id=='2020-03-27-3100373.0-好吃市集'].reset_index(drop=True)

                for each_unique_txn_id in _multi_df.temp_unique_id.unique():
                    _temp_small_multi_df = _multi_df[_multi_df.temp_unique_id==each_unique_txn_id].reset_index(drop=True)
                    # 創建一個對應的dataframe, 內含交易為對應的unique_txn_id
                    _temp_small_multi_df.loc[0, '內容物'] = self._combine_columns(
                            _temp_small_multi_df['內容物'].tolist(), '\n')
                    _temp_small_multi_df.loc[0, '備註'] = self._combine_columns(
                            _temp_small_multi_df['備註'].tolist(), '\n')
                    _temp_small_multi_df.loc[0, '規格'] = self._combine_columns(
                            _temp_small_multi_df['規格'].tolist(), ', ')
                    try:
                        _temp_small_multi_df.loc[0, '金額'] = _temp_small_multi_df['金額'].astype(int).sum()
                    except:
                        pass
                    
                    _temp_df.loc[_temp_df.shape[0]] = \
                            _temp_small_multi_df.loc[0].tolist()
                    
                _temp_df = _temp_df.iloc[:, :-1]
                # 記得把temp_unique_id刪掉 
                self.unique_aggregated_txns = _temp_df
                
                self.aggregated_txns = self.aggregated_txns.iloc[:, :-1]
                
                return self.unique_aggregated_txns
            
            else:
                self.aggregated_txns = self.aggregated_txns.iloc[:, :-1]
                return self.aggregated_txns


    def _integrate_with(self, platform):
        assert platform in self.platforms
        # 先確認要整合的平台存在於平台列表中
        # 用枚舉的方式來根據不同平台報表採取不同動作, 先這樣子未來再找更好的做法

        txn_paths = self._return_txn_path(self.raw_txns_dir, platform)
        # 獲取交易資料檔的路徑

        # 讓這個函式回傳三個東西
        # 1. 是否有找到該平台的交易檔案  boolean
        # 2. 整合過程中, 是否有出現錯誤  boolean
        # 3. 出錯的檔案, 若無則回傳空值  list
        is_found = True
        is_error = False
        exception_files = []

        if platform in ['好吃市集', '生活市集']:

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                
                for txn_path in txn_paths:
                    print('ALICIA: _integrate_with1 : ', txn_path)
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)

                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        print('ALICIA: _integrate_with2 : ', _temp_df.shape)
                        
                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '備註(購買人資料)'].split('/')[0]
                            _receiver_name = _temp_df.loc[each_row_index, '收件人'].split()[0].strip()
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收件地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '電話']
                            _receiver_mobile = _receiver_phone_nbr
                            if not pd.isnull(_temp_df.loc[each_row_index, '實際出貨明細']):
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                _temp_df.loc[each_row_index, '方案*組數'],
                                                                _temp_df.loc[each_row_index, '實際出貨明細']], ', ')
                                _subcontent = _temp_df.loc[each_row_index, '實際出貨明細'].rsplit('/', 1)[1]    
                            
                            else:
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                _temp_df.loc[each_row_index, '方案*組數']],', ').rsplit('*', 1)[0]
                                _subcontent = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                _temp_df.loc[each_row_index, '方案*組數']],', ').rsplit('*', 1)[0]

                            _how_many = int(_temp_df.loc[each_row_index, '方案*組數'].split('*')[-1])
                            _how_much = None
                            _remark = self._combine_columns(['配送時段: ' + _temp_df.loc[each_row_index, '配送時段'],
                                                            _temp_df.loc[each_row_index, '退貨或重複訂單']],
                                                            ', ')
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False

                            #temp_df = _clean_dataframe(pd.read_excel(txn_path))

                            _shipping_link = ''
                            

                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])
                   
                    return is_found, is_error, exception_files
        
        elif platform == '樂天派官網':
            def to_get_subcontent(target_string):
                pattern = re.compile(r'[(]口味選擇:.*[)]|[(]口味:.*[)]|[(]商品規格:.*[)]|[(]規格:.*[)]')
                if len(re.findall(pattern, target_string)) > 0:
                    # 先擷取該部分字串，並去掉首尾的括弧
                    target_string = re.findall(pattern, target_string)[0][1:-1]
                    # 再將冗字去除
                    target_string = ' '.join(target_string.split(':')[1:])
                    if '豬肉' in target_string or '條子' in target_string:
                        target_string = re.sub(re.compile(r'[(]|[)]'), '', target_string)
                    return target_string
                else:
                    if '豬肉' in target_string or '條子' in target_string:
                        target_string = re.sub(re.compile(r'[(]|[)]'), '', target_string)
                    return target_string

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                print('_intergrate_with: ', platform, txn_paths)
                for txn_path in txn_paths:
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        print('before clean', pd.read_excel(txn_path).shape)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        print('Alicia intergrating with 樂天派官網')
                        print('path:', txn_paths)
                        print(_temp_df.shape)
                        print(_temp_df.tail(1).T)

                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '自訂編號']
                            _customer_name = _temp_df.loc[each_row_index, '客戶名稱']
                            _receiver_name = _temp_df.loc[each_row_index, '客戶名稱']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '客戶地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '客戶手機號碼']
                            _receiver_mobile = _receiver_phone_nbr

                            # 先簡單的把內容物直接塞到內容物, 數量塞到數量就好~~~

                            # = = = = = = = = = = = = 以下為理想情況, 先不要這麼做, 太難太亂惹 = = = = = = = = = =
                            # 2020.04.04
                            # 官網商品欄位-購買數量有兩種形式:
                            # 1. 【KeyKey X 川子油蔥醬】配送台灣任選2罐組(260g/罐)(口味:辣味油蔥2罐) - 1
                            #    【Just in bakery】法式冠軍招牌top3麵包組(蜂巢+脆片吐司+蜂蜜貝果)(口味:冠軍蜂巢*1+脆片吐司*1+蜂蜜貝果*2) - 2
                            # 2. 加購－【Just in bakery】蜂巢1個(數量:1) - 1
                            #    加購－穀粉_杏仁5包+芝麻5包(數量:1) - 2

                            # 針對第1種, 我們整理成兩個欄位(內容物 // 數量):
                            # 1.1 【KeyKey X 川子油蔥醬】配送台灣任選2罐組(260g/罐) - 辣味油蔥 // 2
                            # 1.2.1 【Just in bakery】法式冠軍招牌top3麵包組(蜂巢+脆片吐司+蜂蜜貝果) - 冠軍蜂巢 // 2
                            # 1.2.2 【Just in bakery】法式冠軍招牌top3麵包組(蜂巢+脆片吐司+蜂蜜貝果) - 脆片吐司 // 2
                            # 1.2.2 【Just in bakery】法式冠軍招牌top3麵包組(蜂巢+脆片吐司+蜂蜜貝果) - 蜂蜜貝果 // 4

                            # 針對第2種, 我們整理成兩個欄位(內容物 // 數量):
                            # 2.1 加購－【Just in bakery】蜂巢1個 // 1
                            # 2.2 加購－穀粉_杏仁5包+芝麻5包 // 2
                            # = = = = = = = = = = = = 以上為理想情況, 先不要這麼做, 太難太亂惹 = = = = = = = = = =

                        #  def _get_contents_and_quantity()


                            # temp_df = pd.read_excel('raw_txns/export_09Mar20 (樂天派官網).xls')
                            # temp_df = a._clean_dataframe(temp_df)
                            # temp_df.loc[1, '商品名稱'].split('(')[0].strip() + \
                            # ' - ' +

                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _how_many = _temp_df.loc[each_row_index, '購買數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '單價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '備註']
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = to_get_subcontent(_temp_df.loc[each_row_index, '商品名稱'])
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == 'MOMO':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔
                        _file_created_date = self._get_file_created_date(txn_path)

                        # 這裡解決了一個奇怪的bug, 就是不能用相對路徑的方式開啟excel物件by win32元件,
                        # 必須用絕對路徑它才找得到檔案, 故我把所有查找路徑的回傳值都改成絕對路徑
                        #try:
                            # 先輸入密碼試試
                        #    _temp_df = self._turn_wb_into_dataframe(txn_path, 1, self.passwords['MOMO'])
                        #except:
                            # 改不輸入密碼試試
                        #    _temp_df = self._turn_wb_into_dataframe(txn_path, 1, None)

                        #try:
                        #    _temp_df.shape  # 這行只是為了偵測有沒有成功從workbook轉成dataframe
                        #except:
                        #    print(txn_paths, '讀取失敗.')

                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        #_temp_df['訂單編號'] = _temp_df['訂單編號'].apply(lambda x: x.split('-')[0])
                        #_temp_df = self._clean_dataframe(_temp_df)

                        if '貨運公司\n出貨地址' not in _temp_df.columns:
                            print('不是momo去識別化後的訂單')
                            for each_row_index in range(_temp_df.shape[0]):
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                                _customer_name = _temp_df.loc[each_row_index, '訂購人姓名']
                                _receiver_name = _temp_df.loc[each_row_index, '收件人姓名']
                                _paid_after_receiving = False
                                _receiver_address = _temp_df.loc[each_row_index, '收件人地址']
                                _receiver_phone_nbr = _temp_df.loc[each_row_index, '收件人電話']
                                _receiver_mobile = _temp_df.loc[each_row_index, '收件人行動電話']

                                _content = self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                                _temp_df.loc[each_row_index, '單品詳細']],
                                                                ', ')

                                _how_many = _temp_df.loc[each_row_index, '數量']
                                _how_much = _temp_df.loc[each_row_index, '進價(含稅)'].astype(int)
                                _remark = ''
                                _shipping_id = ''
                                _last_charged_date = ''
                                _charged = False
                                _ifsend = False
                                _ifcancel = False
                                _subcontent = _temp_df.loc[each_row_index, '單品詳細']
                                _shipping_link = ''
                                # 寫入資料
                                self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                        _file_created_date,
                                                                                        _txn_id,
                                                                                        _customer_name,
                                                                                        _receiver_name,
                                                                                        _paid_after_receiving,
                                                                                        _receiver_phone_nbr,
                                                                                        _receiver_mobile,
                                                                                        _receiver_address,
                                                                                        _content,
                                                                                        _how_many,
                                                                                        _how_much,
                                                                                        _remark,
                                                                                        _shipping_id,
                                                                                        _last_charged_date,
                                                                                        _charged,
                                                                                        _ifsend,
                                                                                        _ifcancel,
                                                                                        _subcontent,
                                                                                        _shipping_link]
                        else:
                            # 是momo去識別化後的訂單
                            _temp_df['貨運公司\n出貨地址'] = _temp_df['貨運公司\n出貨地址'].apply(lambda x: '(貨運公司出貨地址) ' +  x.replace('新竹貨運\n', ''))
                            print('是momo去識別化後的訂單')
                            for each_row_index in range(_temp_df.shape[0]):
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                                _customer_name = _temp_df.loc[each_row_index, '訂購人姓名']
                                _receiver_name = _temp_df.loc[each_row_index, '收件人姓名']
                                _paid_after_receiving = False
                                _receiver_address = _temp_df.loc[each_row_index, '貨運公司\n出貨地址']
                                _receiver_phone_nbr = ''
                                _receiver_mobile = ''
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                                _temp_df.loc[each_row_index, '單品詳細']],
                                                                ', ')
                                _how_many = _temp_df.loc[each_row_index, '數量']
                                _how_much = _temp_df.loc[each_row_index, '進價(含稅)'].astype(int)
                                _remark = ''
                                _shipping_id = ''
                                _last_charged_date = ''
                                _charged = False
                                _ifsend = False
                                _ifcancel = False
                                _subcontent = _temp_df.loc[each_row_index, '單品詳細']
                                _shipping_link = ''
                                # 寫入資料
                                self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                        _file_created_date,
                                                                                        _txn_id,
                                                                                        _customer_name,
                                                                                        _receiver_name,
                                                                                        _paid_after_receiving,
                                                                                        _receiver_phone_nbr,
                                                                                        _receiver_mobile,
                                                                                        _receiver_address,
                                                                                        _content,
                                                                                        _how_many,
                                                                                        _how_much,
                                                                                        _remark,
                                                                                        _shipping_id,
                                                                                        _last_charged_date,
                                                                                        _charged,
                                                                                        _ifsend,
                                                                                        _ifcancel,
                                                                                        _subcontent,
                                                                                        _shipping_link]

                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])       
                return is_found, is_error, exception_files

        elif platform == 'Yahoo購物中心':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔

                        _file_created_date = self._get_file_created_date(txn_path)

                        # Yahoo購物中心的xls檔其實是html格式, 如果用notepad打開它:
                        # <meta http-equiv='Content-Type' content='text/html; charset=utf-8'>....
                        # 所以不能用pd.read_excel(), 要用pd.read_html()來開啟

                        _temp_df = self._clean_dataframe(pd.read_html(txn_path, header=0)[0])

                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '收件人姓名']
                            _receiver_name = _temp_df.loc[each_row_index, '收件人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收件人地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收件人手機']
                            _receiver_mobile = _receiver_phone_nbr

                        #  def _get_contents_and_quantity()

                            # temp_df = pd.read_excel('raw_txns/export_09Mar20 (樂天派官網).xls')
                            # temp_df = a._clean_dataframe(temp_df)
                            # temp_df.loc[1, '商品名稱'].split('(')[0].strip() + \
                            # ' - ' +

                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _how_many = _temp_df.loc[each_row_index, '數量']
                            # _how_much = _temp_df.loc[each_row_index, '商品成本'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '成本小計'].astype(int)  # Jerry堅持要以總額來做紀錄  20.09.29
                            # _remark = _temp_df.loc[each_row_index, '購物車備註']
                            _remark = ''
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == '東森得易購':
            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))

                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '訂單號碼']
                            _customer_name = _temp_df.loc[each_row_index, '客戶名稱']
                            _receiver_name = _temp_df.loc[each_row_index, '客戶名稱']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '配送地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '室內電話']
                            _receiver_mobile = _temp_df.loc[each_row_index, '客戶電話']

                            _content = self._combine_columns([_temp_df.loc[each_row_index, '商品名稱'],
                                                            _temp_df.loc[each_row_index, '顏色'],
                                                            _temp_df.loc[each_row_index, '款式']],
                                                            ', ')


                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = 0
                            _remark = ''
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = self._combine_columns([_temp_df.loc[each_row_index, '商品名稱'],
                                                            _temp_df.loc[each_row_index, '顏色'],
                                                            _temp_df.loc[each_row_index, '款式']],
                                                            ', ', only_meaningful=True)
                            # 根據20.08.21曉箐的說法，顏色跟款式不會同時有內容在裡面，而且這兩個欄位裡面樂天派不會放進空格。
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files

        elif platform == '亞伯':
            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                try:
                    for txn_path in txn_paths:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = self._combine_columns([_temp_df.loc[each_row_index, '廠商訂單編號'],
                                                            _temp_df.loc[each_row_index, '會員訂單編號']],
                                                            '-')
                            _customer_name = _temp_df.loc[each_row_index, '消費者']
                            _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                            try:
                                _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人連絡電話']
                                _receiver_mobile = _temp_df.loc[each_row_index, '收貨人連絡電話']
                            except:
                                _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人聯絡電話']
                                _receiver_mobile = _temp_df.loc[each_row_index, '收貨人聯絡電話']
                            _content = self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                            _temp_df.loc[each_row_index, '選購規格']],
                                                            ', ')

                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '成本小計'].astype(int)
                            _remark = ''
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            
                            if not pd.isnull(_temp_df.loc[each_row_index, '選購規格']):
                                _subcontent = _temp_df.loc[each_row_index, '選購規格']
                            else:
                                _subcontent = _temp_df.loc[each_row_index, '品名']
                            
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                except Exception as e:
                    print(e)
                    is_error = True
                    exception_files.append(ntpath.split(txn_path)[1])                
            return is_found, is_error, exception_files


        elif platform == 'UDN':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔

                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))


                    # UDN xls有兩種格式 一種第一行是英文標題另一種純中文，如果是英文標題則重讀檔並從第二行讀成標題
                        if '訂單編號' not in _temp_df.keys():
                            _temp_df = self._clean_dataframe(pd.read_excel(txn_path, skiprows = 1))
                            #temp_df = _clean_dataframe(pd.read_excel('raw_txns/Order_20191112092608071(UDN).xls', skiprows = 1))

                            # 英文標題的商品index 跟中文的有些不一致，統一改成中文名的為準
                            # 將配送備註與購買備註合併  lambda是為了讓na+str不會變成na
                            _temp_df.loc[:, '配送備註'] = _temp_df.loc[:, '配送備註'].apply(lambda x: '' if pd.isnull(x) else x) + \
                                                        '   ' + _temp_df.loc[:, '購買備註'].apply(lambda x: '' if pd.isnull(x) else x)
                            _temp_df = _temp_df.rename(columns = {'商品名稱+規格尺寸':'商品名稱','訂購數量':'數量',
                                                                '原售價':'單價','配送備註':'備註/卡片內容'})


                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '訂購人姓名']
                            _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人手機']
                            _receiver_mobile = _receiver_phone_nbr

                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '進貨價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '備註/卡片內容']
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == '台塑':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔

                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))


                    # 台塑 xls有兩種格式 一種第一行是英文標題另一種純中文，如果是英文標題則重讀檔並從第二行讀成標題
                        if '訂單編號' not in _temp_df.keys():
                            _temp_df = self._clean_dataframe(pd.read_excel(txn_path, skiprows = 1))
                            #temp_df = _clean_dataframe(pd.read_excel('raw_txns/Order_20191112092608071(UDN).xls', skiprows = 1))

                            # 英文標題的商品index 跟中文的有些不一致，統一改成中文名的為準
                            # 將配送備註與購買備註合併  lambda是為了讓na+str不會變成na
                            _temp_df.loc[:, '配送備註'] = _temp_df.loc[:, '配送備註'].apply(lambda x: '' if pd.isnull(x) else x) + \
                                                        '   ' + _temp_df.loc[:, '購買備註'].apply(lambda x: '' if pd.isnull(x) else x)
                            _temp_df = _temp_df.rename(columns = {'商品名稱+規格尺寸':'商品名稱','訂購數量':'數量',
                                                                '原售價':'單價','配送備註':'備註/卡片內容'})


                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '訂購人姓名']
                            _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人手機']
                            _receiver_mobile = _receiver_phone_nbr

                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            try:
                                _how_much = _temp_df.loc[each_row_index, '成本價'].astype(int)
                            except Exception as e:
                                print(e)
                                _how_much = _temp_df.loc[each_row_index, '進貨價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '備註/卡片內容']
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == 'LaNew':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path, skiprows = 1))
                        # print('try1', _temp_df.columns)

                        # LaNew第一行疑似為空白，因此預定從第二行開始讀起，萬一找不到【訂單編號】這一欄位，再從第一行讀起
                        if '訂單編號' not in _temp_df.columns:
                            _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                            # print('try2', _temp_df.columns)

                        # 將商品備註與訂單備註合併  lambda是為了讓na+str不會變成na
                        _temp_df.loc[:, '商品備註'] = _temp_df.loc[:, '商品備註'].apply(lambda x: '' if pd.isnull(x) else x) + \
                                                    '   ' + _temp_df.loc[:, '訂單備註'].apply(lambda x: '' if pd.isnull(x) else x)

                        # 將(配送方式)與(地址)結合>>
                        _temp_df.loc[:, '地址'] = _temp_df.loc[:, '配送方式'].apply(lambda x: '' if pd.isnull(x) or x=='' else '(' + x + ') ') + \
                                                    _temp_df.loc[:, '地址'].apply(lambda x: '' if pd.isnull(x) else x)

                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '收件人'] # 因為沒有客戶(購買者)欄位，故以收件人取代
                            _receiver_name = _temp_df.loc[each_row_index, '收件人']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收件人電話']
                            _receiver_mobile = _receiver_phone_nbr

                            _content = _temp_df.loc[each_row_index, '品名']
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '單價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '商品備註']
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _content
                            _shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == 'Friday':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.csv'))
                        # 檢查是否為csv檔

                        _file_created_date = self._get_file_created_date(txn_path)
                        #_temp_df = (pd.read_csv(r'raw_txns\OrderData_43946 - 2020-03-09T111005.304(Friday).csv',engine='python'))
                        _temp_df = self._clean_dataframe(pd.read_csv(txn_path, engine = 'python', encoding='big5'))
                        # 需使用big5編碼才能成功轉譯
                        
                        #if len(_temp_df.columns)==24:
                        #    _temp_df.columns = ['訂單時間', '通知出貨時間', '通知退換貨時間', '訂單編號', '出貨單號', '商品名稱',
                        #                        '商品單價(*數量)', '配送方式', '訂單狀態', '收件人', '收件人地址', '收件人電話', '收件人手機', 
                        #                        '訂單備註', '搭配活動', '訂單類型', '應出貨日期', '規格名稱', '商品原廠型號', '退/換貨原因', 
                        #                        '規格條碼或編號', '規格序號', '提報成本(*數量)', '商品序號']

                        for each_row_index in range(_temp_df.shape[0]):
                            # [1:-1]是要清除儲存裡最前的'符號
                            print(txn_path, each_row_index)
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號'][1:]
                            _customer_name = _temp_df.loc[each_row_index, '收件人'][1:]
                            _receiver_name = _temp_df.loc[each_row_index, '收件人'][1:]
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收件人地址'][1:]
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收件人手機'][1:]
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '商品名稱'][1:]

                        ## 商品單價跟數量和在同一儲存格'商品單價(*數量)'，要做分開處理
                            #_temp_item = _temp_df.loc[each_row_index, '商品單價(*數量)']
                            ## print(_temp_item)
                            #_how_much = int(re.findall(r"^'\d+[(]",_temp_item)[0][1:-1])
                            #_how_many = int(re.findall(r"[(]\d+[)$]",_temp_item)[0][1:-1])

                        # 商品單價跟數量和在同一儲存格'提報成本(*數量)'，要做分開處理
                            _temp_item = _temp_df.loc[each_row_index, '提報成本(*數量)']
                            # print(_temp_item)
                            _how_much = int(re.findall(r"^'\d+[(]",_temp_item)[0][1:-1])
                            _how_many = int(re.findall(r"[(]\d+[)$]",_temp_item)[0][1:-1])

                            _remark = _temp_df.loc[each_row_index, '訂單備註']
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱'][1:]
                            _shipping_link = ''
                            
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files

        
        elif platform == '快車肉乾銷港':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_csv(txn_path))
                        # _temp_df.loc[:, '_temp_subcontent'] = _temp_df['Product Name'].apply(lambda x: x.split('-')[-1] if '-' in x else x.split(' ')[-2])
                        # print('ali: spilt')
                        _temp_df.loc[:, '_temp_subcontent'] = _temp_df['Product Name'].apply(lambda x: x.split('】')[-1].split('-')[-1].strip() if '-' in x else x.split('】')[-1].strip())
                        #_temp_df.loc[:, '_temp_subcontent'] = _temp_df['Product Name'].apply(lambda x: x.split('-')[-1].strip() if '-' in x else x.split(' ')[-2])
                        _temp_df.loc[:, 'customer_name'] = _temp_df['First Name'] + ' ' + _temp_df['Last Name']
                        _temp_df.loc[:, '_temp_remark'] = _temp_df['Order Note'] + '; email:' + _temp_df['Buyer\'s Email Address']
                        _temp_df['Shipping Address'] = _temp_df['Shipping Address'].apply(lambda x: x.replace('\n', ' '))

                        for each_row_index in range(_temp_df.shape[0]):
                            _txn_id = _temp_df.loc[each_row_index, 'Order#']
                            _customer_name = _temp_df.loc[each_row_index, 'customer_name']
                            _receiver_name = _temp_df.loc[each_row_index, 'customer_name']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, 'Shipping Address']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, 'Buyer\'s Contact Number']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, 'Product Name']
                            _how_much = _temp_df.loc[each_row_index, 'Product Price'].astype(int)
                            _how_many = _temp_df.loc[each_row_index, 'Quantity Ordered'].astype(int)

                            _remark = _temp_df.loc[each_row_index, '_temp_remark']
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '_temp_subcontent']
                            _shipping_link = ''
                            
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print('Alicia Integrating', platform, e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files
        
        
        elif platform == '博客來':

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        assert (txn_path.endswith('.xls') or txn_path.endswith('.xlsx') or txn_path.endswith('.xlsm'))
                        # 檢查是否為excel檔
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))


                        for each_row_index in range(_temp_df.shape[0]):
                            # [1:-1]是要清除儲存裡最前的'符號
                            _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '收件人']
                            _receiver_name = _temp_df.loc[each_row_index, '收件人']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '配送地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '聯絡電話']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _how_much = _temp_df.loc[each_row_index, '進貨價'].astype(int)
                            _how_many = _temp_df.loc[each_row_index, '訂購量'].astype(int)

                            _remark = ''
                            _shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _shipping_link = ''
                            
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _receiver_address,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _subcontent,
                                                                                    _shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files

        elif platform == '整合檔':
            # 整合檔跟其他平台最大的差別在於：它是可以直接被整合進資料庫裡的。
            # 因此不需要多做甚麼資料整理，但仍然要清一下各個column，免得有多餘的空白或跳行。

            if len(txn_paths) == 0:
                print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                print(txn_paths)
                for txn_path in txn_paths:
                    try:
                        criteria = ('.xlsx', '.xlsm')
                        assert txn_path.endswith(criteria)
                        # 檢查是否為excel檔
                        # 新增一些資料清理邏輯
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path), strip_only=True)
                        print('clean dataframe successfully.')
                        _temp_df['宅單'][~pd.isnull(_temp_df['宅單'])] = _temp_df['宅單'][~pd.isnull(_temp_df['宅單'])].apply(lambda x: str(x).replace('\'', '').replace('-', ''))
                        _temp_df['貨到付款'][pd.isnull(_temp_df['貨到付款'])] = False
                        _temp_df['地址'][pd.isnull(_temp_df['地址'])] = ''
                        _temp_df['金額'][pd.isnull(_temp_df['金額'])] = 0
                        _temp_df['數量'][pd.isnull(_temp_df['數量'])] = 1
                        _temp_df['已寄出'][pd.isnull(_temp_df['已寄出'])] = False
                        _temp_df['已取消'][pd.isnull(_temp_df['已取消'])] = False
                        _temp_df['規格'][pd.isnull(_temp_df['規格'])] = _temp_df['內容物'][pd.isnull(_temp_df['規格'])]

                        _file_created_date = self._get_file_created_date(txn_path)
                        self.user_uploaded_aggregated_txns = pd.concat([
                            self.user_uploaded_aggregated_txns,
                            _temp_df
                        ], join='inner')

                        self.user_uploaded_aggregated_txns = self._clean_dataframe(
                            pandas_dataframe=self.user_uploaded_aggregated_txns,
                            strip_only=True,
                            to_database_format=True, 
                            dealing_columns=['貨到付款', '回押', '已寄出', '已取消']
                        )

                        # 將讀到的資料賦值予 self.user_uploaded_aggregated_txns
                        # 並且確認一下其欄位內容如同預期的一樣

                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


    def _intergate_all_platforms(self):
        platforms_found, platforms_not_found, exception_files = [], [], []
        # 整合所有訂單後, 回傳有找到的平台跟沒有找到的, 以及有問題的檔案們
        for each_platform in self.platforms:
            # print('_intergate_all_platforms b4', each_platform, self.aggregated_txns)
            is_found, _is_error, sub_exception_files = self._integrate_with(each_platform)
            # print('_intergate_all_platforms after', is_found, _is_error, sub_exception_files, self.aggregated_txns)
            if is_found:
                platforms_found.append(each_platform)
                # print(self.aggregated_txns)
            else:
                platforms_not_found.append(each_platform)
            exception_files.extend(sub_exception_files)
        # 在這邊添加整合檔處理, 或是加在 「_integrate_with()」中一起整合好呢??
        # >> 與其在這裡另闢戰場例外處理"整合檔", 倒不如放在「_integrate_with()」中一起整合, 感覺比較合理,
        #    也不用再另外思考要怎麼確認整合檔有沒有在裡面。       by Tamio @2020.06.24
        return (platforms_found, platforms_not_found, exception_files)


    def generate_shipping_url(self, shipping_number, shipping_company):
        # shipping_company 有兩個選項, "black_cat" 或是 "xinzhu"
        def to_int_in_string_format(target):
            try:
                return str(int(target))
            except:
                return str(target)
        
        if shipping_company == 'xinzhu':
            content= to_int_in_string_format(shipping_number)  # 此處的number經異動後為貨號，非訂單編號
            iv='UXKKWJCP'  # 這個是加密向量，為一常量不會改變
            key=datetime.today()-timedelta(days=121)  # 金鑰為查詢當日日期-121天
            key=key.strftime('%Y%m%d')
            iv, key, content = bytes(iv, 'utf-8'), bytes(key, 'utf-8'), bytes(content, 'utf-8')
            # 加密前先轉byte utf8編碼
            k = pyDes.des(key, pyDes.CBC, iv, padmode=pyDes.PAD_PKCS5)
            encrypt_content = k.encrypt(content)
            encrypt_content=str(base64.b64encode(encrypt_content), encoding='utf-8')
            # 加密後的字串再轉為BASE-64
            url='https://www.hct.com.tw/phone/searchGoods_Main.aspx?no=' + \
                 encrypt_content + '&v=2BD074B07FC5382739EC6B1B88C1E8D4'   # 左邊這個v也是一個常量
            return url

    def try_to_be_int_in_str(self, target):
        condition1 = target is None or pd.isnull(target) or pd.isna(target)
        condition2 = target in ['nan', ' ']
        
        if condition1 or condition2:
            return ''
        try:
            return str(round(int(float(target)), 0))
        except:
            return str(target)


    def make_phone_and_mobile_number_clean(self, raw_number):
        # 會回傳0911-111-111或是02-2222-2222 #222之類的格式    

        raw_number = self.try_to_be_int_in_str(raw_number)
        raw_number = re.sub(re.compile(r'[- －]'), '', raw_number)
        raw_number = raw_number.replace('#', ' #')
        
        mobile_pattern = re.compile(r'^09.*|^9.*')
        if len(re.findall(mobile_pattern, raw_number)):
            if raw_number[0] == '9':
                raw_number = '0' + raw_number
            raw_number = raw_number[:4] + '-' + raw_number[4:7] + '-' + raw_number[7:]
            return raw_number
        elif len(raw_number) > 5:
            if raw_number[0] != '0':
                raw_number = '0' + raw_number
            raw_number = raw_number[:2] + '-' + raw_number[2:6] + '-' + raw_number[6:]
            return raw_number
        else:
            return raw_number

    




if __name__ == '__main__':
    import re, pandas as pd
    os.chdir('/mnt/c/Users/User/Desktop/20200713_HP_Project')
    # pattern = re.compile(r'.{0,6}orders\s*[(]{0,1}\d*[)]{0,1}\s*.csv|.{0,6}orders\s*[(]{0,1}\d*[)]{0,1}\s*.xls[x]{0,1}')
    

    
    
    
    #a = ALICIA()
    #a.raw_txns_dir = 'test_folder'
    #a.decr_raw_txns_dir = 'test_folder'
    
    #a._integrate_with('MOMO')
    #print(a.aggregated_txns)
    #a.pre_clean_raw_txns()
    #print(a.aggregated_txns)


    

# MO的開檔密碼:happypi02
# 東森解鎖:52464493
# 亞伯開檔密碼:524644932186
