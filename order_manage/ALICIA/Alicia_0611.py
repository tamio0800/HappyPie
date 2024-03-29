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
from datetime import datetime, timedelta, date as date_function
import pyDes
import base64
from order_manage.models import Qingye_Niancai_raw_record


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
        self.vendors = self._get_vendors()   # 回傳兩種型態的vendors
        

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
        # 先移動需要密碼的那幾個平台, 因為處理方式略為不同
        assert from_dir != to_dir
        for encrypted_platform in ['MOMO', '亞伯', '東森得易購', '特力家Online店']:
            encrypted_txn_files = self._return_txn_path(from_dir, encrypted_platform)
            if encrypted_platform == '亞伯':
                encrypted_txn_files = [_ for _ in encrypted_txn_files if 'shipmentReport' not in _]
            
            # print('encrypted_txn_files', encrypted_txn_files)
            for each_encrypted_txn_file in encrypted_txn_files:
                # 下面這句是將路徑/檔案名 分解成 路徑, 檔案名, 使用ntpath在linux與windows環境下都可以正常運作
                _, tail_of_file = ntpath.split(each_encrypted_txn_file)
                # print(f'Encrypted files: {each_encrypted_txn_file}')
                if self.check_if_the_xl_file_is_encrypted(each_encrypted_txn_file):
                    try:
                        # 嘗試進行解密
                        the_file = ms.OfficeFile(open(each_encrypted_txn_file, 'rb'))
                        the_file.load_key(password=self.password_dict[encrypted_platform])
                        the_file.decrypt(open(os.path.join(to_dir, tail_of_file), 'wb'))
                    except Exception as e:
                        the_file = ms.OfficeFile(open(each_encrypted_txn_file, 'rb'))
                        the_file.load_key(password='')
                        the_file.decrypt(open(os.path.join(to_dir, tail_of_file), 'wb'))
                        pass

                    try:
                        os.unlink(each_encrypted_txn_file)
                    except Exception as e:
                        print(f'move_files_and_decrypt_them EXCEPTION(2): {e}')

        # 把剩下的檔案移一移, 包括原先就沒有加密的跟理論上會加密但沒有加密的那些檔案
        for each_file in os.listdir(from_dir):
            os.rename(
                os.path.join(from_dir, each_file),
                os.path.join(to_dir, each_file)
            )
            print(f"成功將檔案解密並且移動：{each_file}  至 {to_dir}")

    def delete_files_in_the_folder(self, folder_path):
        for each_file in os.listdir(folder_path):
            # 盡可能刪除裡面所有的檔案
            try:
                if not each_file.endswith('fortracked'):
                    os.unlink(os.path.join(folder_path, each_file))
                    print(each_file)
            except:
                pass

    def force_float_to_be_int_and_to_string(self, target):
            _result = target
            try:
                _result = str(int(target))
            except:
                _result = str(target)
            return _result

    def pre_clean_raw_txns(self, unique_ids_in_database=None):
        if self.aggregated_txns.shape[0] > 0:
            self.aggregated_txns['訂單編號'] = self.aggregated_txns['訂單編號'].apply(self.force_float_to_be_int_and_to_string)
            # self.aggregated_txns 至少要有東西再清理
            # 以通路 + 編號 + 內容物作為暫時的unique_id,
            # 來作為A交易在昨天與今天一起被重複匯進來的處理機制
            self.aggregated_txns['內容物'] = self.aggregated_txns['內容物'].apply(lambda x: re.sub(r'神老師推薦》\s{0,4}', '神老師推薦》', x))
            self.aggregated_txns['內容物'] = self.aggregated_txns['內容物'].apply(lambda x: re.sub(r'《青葉臺菜X神老師推薦》\s{0,4}冰箱', '《神老師推薦》冰箱', x))
            self.aggregated_txns['規格'] = self.aggregated_txns['規格'].apply(lambda x: re.sub(r'輕滋', ' ', x))
            self.aggregated_txns['規格'] = self.aggregated_txns['規格'].apply(lambda x: re.sub(r'到貨日\s{0,2}:\s{0,2}', ' ', x))

            try:
                print(f'pre_clean_raw_txns1 {self.aggregated_txns}')
                self.aggregated_txns.loc[:, 'pre_clean_unique_id'] = self.aggregated_txns['通路'] + '|' + \
                    self.aggregated_txns['訂單編號'].apply(self.force_float_to_be_int_and_to_string) + '|' + \
                    self.aggregated_txns['供應商'] + '|' + self.aggregated_txns['內容物']
                    # self.aggregated_txns['訂單編號'].astype(str) + '-' + \
                self.aggregated_txns = self.aggregated_txns.drop_duplicates(subset='pre_clean_unique_id', keep='first')
                self.aggregated_txns = self.aggregated_txns.sort_values('pre_clean_unique_id').reset_index(drop=True)
                self.aggregated_txns = self.aggregated_txns.drop(['pre_clean_unique_id'], axis=1)
                print(f'pre_clean_raw_txns2 {self.aggregated_txns}')
            except Exception as e:
                print(f'pre_clean_raw_txns_ERROR {e}')

            
            # >> 以通路 + 供應商 + 編號 作為unique_id  2020.11.22
            # 2021.01.15 為官網的青葉年菜另做處理
            try:
                # print(f'before making unique_id col: {self.aggregated_txns.columns}')
                official_qingye_txn_ids = self.aggregated_txns[
                    (self.aggregated_txns['通路'] == '樂天派官網') &
                    ((self.aggregated_txns['內容物'].str.contains('青葉臺菜')) | 
                    (self.aggregated_txns['內容物'].str.contains('青葉台菜')))]['訂單編號'].unique()
                self.aggregated_txns.loc[:, 'unique_id'] = \
                    self.aggregated_txns['通路'] + '|'\
                    + self.aggregated_txns['供應商'] + '|'\
                    + self.aggregated_txns['訂單編號']
                print(f'pre_clean_raw_txns3 {self.aggregated_txns.shape}')
                self.aggregated_txns.loc[self.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids), 'unique_id'] = \
                    self.aggregated_txns[self.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                    + self.aggregated_txns[self.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                    + self.aggregated_txns[self.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string) + '|'\
                    + self.aggregated_txns[self.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids)]['內容物'][-20:]
                print(f'pre_clean_raw_txns4 {self.aggregated_txns.shape}')
                #self.aggregated_txns.loc[:, 'unique_id'] = \
                #    self.aggregated_txns['通路'] + '|'\
                #    + self.aggregated_txns['供應商'] + '|'\
                #    + self.aggregated_txns['訂單編號'].apply(self.force_float_to_be_int_and_to_string)
                #print(f'after making unique_id col: {self.aggregated_txns.columns}')
                #print(f'pre_clean_raw_txns_making_unique_ids: {self.aggregated_txns.unique_id}')
            except Exception as e:
                print(f'pre_clean_raw_txns_making_unique_ids error: {e}')
            #self.aggregated_txns.loc[:, 'unique_id'] = self.aggregated_txns['通路'] + '|' + \
            #    self.aggregated_txns['供應商'] + '|' + \
            #    self.aggregated_txns['訂單編號'].apply(self.force_float_to_be_int_and_to_string)

            # 2021.01.10 >> 針對 輕滋百蔬宴米糕 做特別處理
            self.aggregated_txns.loc[:, '規格'] = \
                self.aggregated_txns.loc[:, '規格'].apply(lambda x: re.sub(r'輕滋百蔬宴米糕', ' 百蔬宴米糕', x)).apply(lambda x: re.sub(r'加購-錵魚一夜干', '加購 - 錵魚一夜干', x))

            # 把Qingye 的 txn_id + content + vendor 當作unique_id做比對
            qingye_unique_string = \
                [f'{i}-{j}' for i, j in Qingye_Niancai_raw_record.objects.values_list('txn_id', 'content')]
            if len(qingye_unique_string):
                # 如果有值再比對就好，這裡要剔除裡面已經有的資料，避免重複計算
                repeated_index = \
                    self.aggregated_txns[
                        (self.aggregated_txns['通路'] == '樂天派官網') &
                        (self.aggregated_txns['訂單編號'] + '-' + self.aggregated_txns['內容物']).isin(qingye_unique_string)].index
                self.aggregated_txns = self.aggregated_txns[~self.aggregated_txns.index.isin(repeated_index)]
            print(f'pre_clean_raw_txns5 {self.aggregated_txns.shape}')
            #print(f'check in pre_clean_function 1: {self.aggregated_txns}')
            # 針對亞伯做特殊處理
            yabo_part = self.aggregated_txns[self.aggregated_txns['通路']=='亞伯']
            non_yabo_part = self.aggregated_txns[~self.aggregated_txns.index.isin(yabo_part.index)]
            print('pre_clean_raw_txns 1.5:  Done None Yabo Part!')

            if yabo_part.shape[0] > 0:
                print('pre_clean_raw_txns 2:  Found Yabo!')
                #yabo_part.to_excel('pre_clean_raw_txns2.1_yabo_part.xlsx', index=False)
                _temp_df = pd.DataFrame(columns=yabo_part.columns)
                yabo_part.loc[:, 'unique_id'] = yabo_part['unique_id'].apply(lambda x: '|'.join(x.split('-')[:-1]))
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
                # print('pre_clean_raw_txns 2.1: ', _temp_df.shape)
                #_temp_df.to_excel('pre_clean_raw_txns2.2_temp_df.xlsx', index=False)
                self.aggregated_txns = pd.concat([non_yabo_part, _temp_df])  # 將兩者合併
                print('pre_clean_raw_txns 3:  Done Yabo Part!')

            if unique_ids_in_database is not None and len(unique_ids_in_database) > 0:
                # user有傳值進來
                self.aggregated_txns = \
                    self.aggregated_txns[~self.aggregated_txns.unique_id.isin(unique_ids_in_database)].reset_index(drop=True)
            #print(f'check in pre_clean_function 2: {self.aggregated_txns}')
            print('pre_clean_raw_txns 4:  Done Whole Part!')

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
        # not_user_uploaded_df指的是user從各個平台下載下來的原始訂單資料，
        # user_uploaded_df則是Alicia整合後的訂單再上傳
        def clean_number_like_columns(df):
            df['訂單編號'] = df['訂單編號'].apply(self.try_to_be_int_in_str).apply(lambda x: x.replace('\'', ''))
            df['常溫宅單編號'] = df['常溫宅單編號'].apply(lambda x: re.sub(re.compile(r'[- －]'), '', str(x))).apply(self.try_to_be_int_in_str)
            df['低溫宅單編號'] = df['低溫宅單編號'].apply(lambda x: re.sub(re.compile(r'[- －]'), '', str(x))).apply(self.try_to_be_int_in_str)
            df['手機'] = df['手機'].apply(self.make_phone_and_mobile_number_clean)
            df['電話'] = df['電話'].apply(self.make_phone_and_mobile_number_clean)
            return df
        
        if not_user_uploaded_df is not None:
            if user_uploaded_df is not None:
                #print(f'combine_aggregated_txns_and_user_uploaded_aggregated_txns_user_uploaded_df {user_uploaded_df}')
                #print(f'combine_aggregated_txns_and_user_uploaded_aggregated_txns_not_user_uploaded_df {not_user_uploaded_df}')
                
                not_user_uploaded_df.loc[:, 'unique_id'] = \
                    not_user_uploaded_df['通路'] + '|' + not_user_uploaded_df['供應商'] + '|' + \
                        not_user_uploaded_df['訂單編號'].apply(self.try_to_be_int_in_str)

                user_uploaded_df.loc[:, 'unique_id'] = \
                    user_uploaded_df['通路'] + '|' + user_uploaded_df['供應商'] + '|' + \
                    user_uploaded_df['訂單編號'].apply(self.try_to_be_int_in_str)

                not_user_uploaded_df = not_user_uploaded_df[
                        pd.to_datetime(not_user_uploaded_df['抓單日']) > (pd.to_datetime(not_user_uploaded_df['抓單日'])  - pd.Timedelta(days=31))
                    ]
                _temp_df = pd.concat([not_user_uploaded_df, user_uploaded_df], join='inner').reset_index(drop=True)
                '''official_qingye_txn_ids = not_user_uploaded_df[
                    (not_user_uploaded_df['通路'] == '樂天派官網') &
                    ((not_user_uploaded_df['內容物'].str.contains('青葉臺菜')) | 
                    (not_user_uploaded_df['內容物'].str.contains('青葉台菜')))]['訂單編號'].unique()
                print(f'inside_combine_aggregated_txns 1: {not_user_uploaded_df}')
                print(f'inside_combine_aggregated_txns 2: {official_qingye_txn_ids}')
                print(f'inside_combine_aggregated_txns 3: {not_user_uploaded_df.columns}')
                not_user_uploaded_df.loc[:, 'unique_id'] = \
                    not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                        + not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                        + not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string)
                
                try:
                    
                    print(f'inside_combine_aggregated_txns 4: {not_user_uploaded_df.columns}')
                    not_user_uploaded_df.loc[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids), 'unique_id'] = \
                        not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                        + not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                        + not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string) + '|'\
                        + not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['內容物'][-20:]
                    print(f'inside_combine_aggregated_txns 5: {not_user_uploaded_df}')
                    
                    #not_user_uploaded_df.loc[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids), 'unique_id'] = \
                    #    not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                    #    + not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                    #    + not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string)
                    print(f'inside_combine_aggregated_txns 6: {not_user_uploaded_df}')
                    print(f'inside_combine_aggregated_txns7: {not_user_uploaded_df.unique_id}')
                    
                    # 再增加一個條件以加速資料寫入的流程>> 1個月以前的交易不做更新(直接從這次的batch中排除)
                    
                except Exception as e:
                    print(f'Exception in combine_aggregated 1: {e}')
                
                try:
                    if user_uploaded_df.shape[0]:
                        user_uploaded_df.loc[:, 'unique_id'] = ''
                        official_qingye_txn_ids = user_uploaded_df[
                            (user_uploaded_df['通路'] == '樂天派官網') &
                            ((user_uploaded_df['內容物'].str.contains('青葉臺菜')) | 
                            (user_uploaded_df['內容物'].str.contains('青葉台菜')))]['訂單編號'].unique()

                        user_uploaded_df.loc[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids), 'unique_id'] = \
                            user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                            + user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                            + user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string) + '|'\
                            + user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['內容物'][-20:]

                        user_uploaded_df.loc[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids), 'unique_id'] = \
                            user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                            + user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                            + user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string)
                        
                        not_user_uploaded_df = \
                            not_user_uploaded_df[~not_user_uploaded_df.unique_id.isin(user_uploaded_df.unique_id)]

                        _temp_df = pd.concat([not_user_uploaded_df, user_uploaded_df], join='inner').reset_index(drop=True)
                    else:
                        _temp_df = not_user_uploaded_df

                
                # user_uploaded_df.to_excel('1124_user_uploaded_df.xlsx', index=False)
                
                        
                        #print(f'combine_aggregated_txns_and_user_uploaded_aggregated_txns {not_user_uploaded_df.columns}')
                except Exception as e:
                    print(f'Exception in combine_aggregated 2: {e}')
                official_qingye_txn_ids = not_user_uploaded_df[
                    (not_user_uploaded_df['通路'] == '樂天派官網') &
                    ((not_user_uploaded_df['內容物'].str.contains('青葉臺菜')) | 
                    (not_user_uploaded_df['內容物'].str.contains('青葉台菜')))]['訂單編號'].unique()
                not_user_uploaded_df.loc[:, 'unique_id'] = ''

                not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)].loc[:, 'unique_id'] = \
                    not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                    + not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                    + not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string) + '|'\
                    + not_user_uploaded_df[not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['內容物'][-20:]

                not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)].loc[:, 'unique_id'] = \
                    not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                    + not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                    + not_user_uploaded_df[~not_user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string)

                official_qingye_txn_ids = user_uploaded_df[
                    (user_uploaded_df['通路'] == '樂天派官網') &
                    ((user_uploaded_df['內容物'].str.contains('青葉臺菜')) | 
                    (user_uploaded_df['內容物'].str.contains('青葉台菜')))]['訂單編號'].unique()
                user_uploaded_df.loc[:, 'unique_id'] = ''

                user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)].loc[:, 'unique_id'] = \
                    user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                    + user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                    + user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string) + '|'\
                    + user_uploaded_df[user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['內容物'][-20:]

                user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)].loc[:, 'unique_id'] = \
                    user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['通路'] + '|'\
                    + user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['供應商'] + '|'\
                    + user_uploaded_df[~user_uploaded_df['訂單編號'].isin(official_qingye_txn_ids)]['訂單編號'].apply(self.force_float_to_be_int_and_to_string)
                print(f'combine_aggregated_txns_and_user_uploaded_aggregated_txns {not_user_uploaded_df.columns}')'''
                #  接著要整理一下，如果user_uploaded_df裡有的交易，就從not_user_uploaded_df中刪除
                
                return clean_number_like_columns(_temp_df)
            else:
                return clean_number_like_columns(not_user_uploaded_df)
        else:
            if user_uploaded_df is not None:
                return clean_number_like_columns(user_uploaded_df)
            else:
                return None

    
    def to_one_unique_id_df_after_kash(self, dataframe_with_unique_id_column, linked_symbol=',\n'):
        ## 這個函示之後要改個名字，顯現它的功能其實是依照『供應商』來將訂單資料分拆/合併

        # unique_id 理想上由  通路-訂單編號  三個元素構成
        # 此函式是為了將含有多個"unique_id"的dataframe整合成真正的unique_id
        # linked_symbol是連接符號, 用以連接複數"unique_id"的內容物們, 如:
        # 0003: 冰淇淋任選2件/香草口味
        # 0003: 冰淇淋任選2件/草莓口味
        # >> 0003: 冰淇淋任選2件/香草口味【連接符號】冰淇淋任選2件/草莓口味
        
        assert dataframe_with_unique_id_column.shape[0] > 0
        # dataframe至少要有東西再丟進來清理
        '''dataframe_with_unique_id_column.loc[:, 'Alicia訂單編號'] = ''
        # 新增『Alicia訂單編號』欄位, 再將此欄位放到『訂單編號後面』
        dataframe_with_unique_id_column = dataframe_with_unique_id_column[
            ['通路', '抓單日', '修訂出貨日', '最終出貨日', '訂單編號', 'Alicia訂單編號', '訂購人', 
            '收件人', '貨到付款', '電話', '手機', '地址', '內容物', '數量', '金額', '備註', '宅單', 
            '最後回押日', '回押', '已寄出', '已取消', '供應商', '規格', '貨運連結', 'unique_id']
        ]'''
        # dataframe_with_unique_id_column.rename(columns={'訂單編號': '原始訂單編號'}, inplace=True)

        #if dataframe_with_unique_id_column.shape[0] != \
        #    len(set(dataframe_with_unique_id_column['原始訂單編號'] + '-' + dataframe_with_unique_id_column['供應商'])):
            
        if dataframe_with_unique_id_column.shape[0] != len(dataframe_with_unique_id_column.unique_id.unique()):
            # dataframe 長度與 其中的 unique_id 長度不同, 代表需要進行整合歸戶(unique_id)
            _temp_df = dataframe_with_unique_id_column[dataframe_with_unique_id_column.unique_id.apply(
                lambda x: True if dataframe_with_unique_id_column[dataframe_with_unique_id_column.unique_id==x].shape[0] == 1 else False)]
            _multi_df = dataframe_with_unique_id_column[~dataframe_with_unique_id_column.unique_id.isin(_temp_df.unique_id)].reset_index(drop=True)
            # 將只有1個row的交易移到_temp_df中, 剩下的放到_multi_df中
            # print(f'_temp_df1: {_temp_df}')
            try:
                for each_unique_txn_id in _multi_df['訂單編號'].unique():
                    tdf = dataframe_with_unique_id_column[dataframe_with_unique_id_column['訂單編號']==each_unique_txn_id]
                    if sum(tdf['通路'].str.contains('樂天派官網')):
                        if sum(tdf['內容物'].str.contains('青葉台菜')) + sum(tdf['內容物'].str.contains('青葉臺菜')):
                            # 如果其中含有任一字眼
                            _temp_df = pd.concat([tdf, _temp_df]).reset_index(drop=True)
                        _multi_df = _multi_df[_multi_df['訂單編號'] != each_unique_txn_id].reset_index(drop=True)
            except Exception as e:
                print(f'to_one_unique_id_df_after_kash error: {e}')
                exit()

            # print(f'_temp_df2: {_temp_df}')

            if _multi_df.shape[0]:
                for each_unique_txn_id in _multi_df.unique_id.unique():
                    _temp_small_multi_df = _multi_df[_multi_df.unique_id==each_unique_txn_id].reset_index(drop=True)
                    # 創建一個對應的dataframe, 內含交易為對應的unique_txn_id
                    _temp_small_multi_df.loc[0, '內容物'] = self._combine_columns(
                            _temp_small_multi_df['內容物'].tolist(), linked_symbol)
                    _temp_small_multi_df.loc[0, '備註'] = self._combine_columns(
                            list(
                                filter(
                                    lambda x: pd.isnull(x) == False, 
                                    list(set(_temp_small_multi_df['備註'].tolist())))
                                ), linked_symbol)
                    _temp_small_multi_df.loc[0, '規格'] = self._combine_columns(
                            _temp_small_multi_df['規格'].tolist(), ', ')
                    _temp_small_multi_df.loc[0, '供應商'] = self._combine_columns(
                            list(
                                filter(
                                    lambda x: len(x) > 0, 
                                    list(set(_temp_small_multi_df['供應商'].tolist())))
                                ), ', ')
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
                 '樂天派官網': re.compile(r'.*export_[0-9]{2}\w{3}[0-9]{2}\s{0,2}.*xls[x]{0,1}$|.*2[0-9]{7}_export_default.{0,6}.xls[x]{0,1}'),
                 'MOMO': re.compile(r'[A-Z]\d+_\d_\d+_\d+_[20]\d+.xls|\S+\d+\s{0,2}[(]MOMO[)].xls|.*訂單查詢-第三方物流.*xls[x]{0,1}$|[A-Z]\d+_\d_\d+_[20]\d+.{0,6}.xls'),
                 '亞伯': re.compile(r'a52464493_PoDetail_\d+.xls|\S+PoDetail_\d+\s{0,2}[(]亞伯[)].xls[x]{0,1}|a52464493_shipmentReport_\d+.{0,6}.xls[x]{0,1}'),
                 '東森得易購': re.compile(r'^[a-z0-9]{8}_20\d+.{0,6}.xls'),
                 'Yahoo購物中心': re.compile(r'^delivery - [0-9]{4}-[0-9]{2}-[0-9]{2}\S+\s{0,2}[(]YAHOO購物中心[)].xls|^delivery - [0-9]{4}-[0-9]{2}-[0-9]{2}\S+\s{0,2}.{0,6}.xls|^delivery\s{0,2}[(]\d{0,3}[)].xls[x]{0,1}'),
                 'UDN': re.compile(r'^Order_2[0-9]{16}[(][Uu][Dd][Nn][)].{0,6}'),
                 'Friday': re.compile(r'^OrderData_[0-9]{5} - 2[0-9]{3}-[0-9]{2}-\S+.{0,6}.csv|^OrderData_[0-9]{5}\s{0,2}[(]\d{0,3}[)].csv'),
                 '博客來': re.compile(r'^take_order_2[0-9]{13}\s{0,2}[(]博客來[)].xls|^take_order_2[0-9]{13}\s{0,2}.{0,6}.xls'),
                 '台塑': re.compile(r'^Order_2[0-9]{16}[(]台塑[)]'),
                 '整合檔': re.compile(r'.*20[0-9]{6}-[0-9]{6}_.*整合檔.*.xls[x]{0,1}'),
                 'LaNew': re.compile(r'複{0,1}本{0,1}[_ ]{0,1}訂{0,1}單{0,1}接{0,1}單{0,1}[_ ]{0,1}[A-Z]{3}\d{2}_2[0-9]{3}[01][0-9][0123][0-9].{0,6}.xls[x]{0,1}'),
                 '快車肉乾銷港': re.compile(r'.{0,6}orders\s*[(]{0,1}\d*[)]{0,1}\s*.csv|.{0,6}orders\s*[(]{0,1}\d*[)]{0,1}\s*.{0,6}.xls[x]{0,1}'),
                 '特力家Online店': re.compile(r'複{0,1}本{0,1}[_ ]{0,1}TLW\d{27}\s{0,1}[(]{0,1}\d*[)]{0,1}.xls'),
                 '龍哥': re.compile(r'^2\d{7}拋單_\S{1,10}.xls[x]{0,1}'),
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


    def _get_vendors(self):
        # 這樣寫是為了更好的管理供應商名稱
        # 回傳(a, b)兩個dicts, 都長得像這樣 >>
        # {'堅果先生': ['堅果先生', 'MR.NUTS'],
        # '拿破崙先生': ['拿破崙先生',]...}
        _prior_temp = dict()
        _minor_temp = dict()
        for vendor_type in ['prior_vendors.txt', 'minor_vendors.txt']:
            with open(os.path.join(self.data_path, vendor_type), 'r', errors='ignore') as r:
                for each_row in r:
                    contents = each_row.strip().split(',')
                    if vendor_type == 'prior_vendors.txt':
                        if len(contents) == 1:
                            _prior_temp[contents[0]] = [contents[0],]
                        else:
                            _prior_temp[contents[0]] = contents
                    else:
                        if len(contents) == 1:
                            _minor_temp[contents[0]] = [contents[0],]
                        else:
                            _minor_temp[contents[0]] = contents
        return (_prior_temp, _minor_temp)

    def who_is_vendor_from_this_product(self, product_name):
        _prior_vendors, _minor_vendors = self.vendors
        _result = list()
        for k, v in _prior_vendors.items():
            # 先檢查優先級別較高的供應商

            check_if_product_name_contains_any_of_these_value = \
                any(_ in product_name for _ in v)
            if check_if_product_name_contains_any_of_these_value == True:
                _result.append(k)

        if len(_result) > 0:
            return ','.join(_result)
        else:
            for k, v in _minor_vendors.items():
                check_if_product_name_contains_any_of_these_value = \
                    any(_ in product_name for _ in v)
                if check_if_product_name_contains_any_of_these_value == True:
                    _result.append(k)
            # 萬一都沒檢查到...
        if len(_result) > 0:
            return ','.join(_result)
        else:
            return ''

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
        print(f"_clean_dataframe 確認是否符合標準")
        assert type(pandas_dataframe) is pd.core.frame.DataFrame
        print(f"_clean_dataframe 確認是否符合標準 >> 符合")
        columns_cannot_be_ffill = [
            '載具編號', '備註', '轉帳帳號', '室內電話', '預計配送日', '買家備註', '賣家備註', '客服備註',
            '購物車備註', '商品屬性', '活動序號', '配送備註', '購買備註', '特標語', 'shipxrem', 'xrem', 'spslgn',
            '指交日期', 'sstockdat', '訂單備註', '搭配活動', '退/換貨原因', '宅單備註', '配送訊息', '約定配送日',
            '商品寄件人聯絡電話'
            ]
        # 檢查輸入值是否為pandas dataframe CLASS
        for each_col in pandas_dataframe.columns:
            print(f"檢測each_col >> {each_col}")
            # print('ALICIA _clean_dataframe 1: ', each_col)
            if not strip_only:
                if each_col not in columns_cannot_be_ffill:
                    pandas_dataframe.loc[:, each_col] = pandas_dataframe.loc[:, each_col].fillna(method='ffill')
                    # 為了處理merged cell會為null值的問題,
                    # 將每個row由上往下最後一次看到的非null值分配給下方的null值
            else:
                try:
                    pandas_dataframe.loc[:, each_col] = pandas_dataframe.loc[:, each_col].apply(lambda x: '' if pd.isnull(x) else x)
                    # pandas_dataframe.loc[:, each_col] = pandas_dataframe.loc[:, each_col].apply(lambda x: x.strip() if not pd.isnull(x) else '')
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
                if not (pd.isnull(_element) or len(_element) == 0 or _element == '共同'):
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
                if not (pd.isnull(_element) or len(_element) == 0 or _element == '共同'):
                    if _ == 0:
                        _temp += str(_element)
                    else:
                        _temp += linked + str(_element)
            if len(_temp) == 0:
                # 沒看到什麼重要的資訊
                _temp = combine_1_dim_array[0].strip()
        return _temp    


    def _get_file_created_date(self, file_path):
        # return time.strftime('%Y-%m-%d', time.gmtime(os.path.getmtime(file_path)))
        # 改成以當日日期為主
        return date_function.today().strftime("%Y-%m-%d")


    
    def _get_unique_txns(self):
        if self.aggregated_txns.shape[0]:
            # 上面那行代表 aggregated_txns dataframe裡面有資料
            self.aggregated_txns.loc[:, 'temp_unique_id'] = self.aggregated_txns['抓單日'] + '|' + \
                self.aggregated_txns['訂單編號'].apply(self.try_to_be_int_in_str) + '|' + \
                self.aggregated_txns['通路']                                                                                      
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
                # print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                
                for txn_path in txn_paths:
                    #print('ALICIA: _integrate_with1 : ', txn_path)
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)

                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        #print('ALICIA: _integrate_with2 : ', _temp_df.shape)
                        
                        for each_row_index in range(_temp_df.shape[0]):
                            try:
                                if '訂單編號' in _temp_df.columns:
                                    _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                                else:
                                    _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '商品訂單編號'])
                            except Exception as e:
                                print(e)
                                if '訂單編號' in _temp_df.columns:
                                    _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                                else:
                                    _txn_id = _temp_df.loc[each_row_index, '商品訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '備註(購買人資料)'].split('/')[0]
                            _receiver_name = _temp_df.loc[each_row_index, '收件人'].split()[0].strip()
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收件地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '電話']
                            _receiver_mobile = _receiver_phone_nbr
                            if not pd.isnull(_temp_df.loc[each_row_index, '實際出貨明細']):
                                if _temp_df.loc[each_row_index, '實際出貨明細'][0] == '(' and _temp_df.loc[each_row_index, '實際出貨明細'][-1] == ')':
                                    _content = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                _temp_df.loc[each_row_index, '方案*組數']],', ').rsplit('*', 1)[0]
                                    _subcontent = _temp_df.loc[each_row_index, '方案*組數'].strip()
                                else:
                                    _content = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                    _temp_df.loc[each_row_index, '方案*組數'],
                                                                    _temp_df.loc[each_row_index, '實際出貨明細']], ', ')
                                    _subcontent = _temp_df.loc[each_row_index, '實際出貨明細'].rsplit('/', 1)[1]    
                            
                            else:
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                _temp_df.loc[each_row_index, '方案*組數']],', ').rsplit('*', 1)[0]
                                _subcontent = self._combine_columns([_temp_df.loc[each_row_index, '檔次名稱'],
                                                                _temp_df.loc[each_row_index, '方案*組數']],', ').rsplit('*', 1)[0]
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = int(_temp_df.loc[each_row_index, '方案*組數'].split('*')[-1])
                            _how_much = None
                            if '配送時段' in _temp_df.columns:
                                _remark = self._combine_columns(['配送時段: ' + _temp_df.loc[each_row_index, '配送時段'],
                                                                _temp_df.loc[each_row_index, '退貨或重複訂單']],
                                                                ', ')
                            else:
                                _remark = self._combine_columns([_temp_df.loc[each_row_index, '退貨或重複訂單']], ', ')
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            #temp_df = _clean_dataframe(pd.read_excel(txn_path))
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])
                    return is_found, is_error, exception_files

        elif platform == '特力家Online店':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path, skiprows=1))
                        
                        for each_row_index in range(_temp_df.shape[0]):
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單號碼'])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單號碼']
                            _customer_name = _temp_df.loc[each_row_index, '收件人姓名']
                            _receiver_name = _temp_df.loc[each_row_index, '收件人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '電話']
                            _receiver_mobile = _temp_df.loc[each_row_index, '手機']
                            _content = _temp_df.loc[each_row_index, '網站品名']
                            _subcontent = _content
                            if not pd.isnull(_temp_df.loc[each_row_index, '規格']):
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '網站品名'],
                                                                _temp_df.loc[each_row_index, '規格']], ', ')
                                _subcontent = _temp_df.loc[each_row_index, '規格']
                            else:
                                _content = _temp_df.loc[each_row_index, '網站品名']
                                _subcontent = _content

                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = int(_temp_df.loc[each_row_index, '數量'])
                            _how_much = int(_temp_df.loc[each_row_index, '成本(未稅)'])
                            _remark = _temp_df.loc[each_row_index, '訂單備註']
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            #temp_df = _clean_dataframe(pd.read_excel(txn_path))
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
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
                # print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                # 有樂天派官網的資料
                # 確認一下資料有沒有留存在青葉資料庫裏面，有的話要剔除

                # print('_intergrate_with: ', platform, txn_paths)
                for txn_path in txn_paths:
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        #print('before clean', pd.read_excel(txn_path).shape)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        for each_row_index in range(_temp_df.shape[0]):
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '自訂編號'])
                            except Exception as e:
                                print(e)
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
                            # print(f"_content before  {_temp_df.loc[each_row_index, '商品名稱']}")
                            _content = re.sub(r'神老師推薦》\s{0,4}', '神老師推薦》', _temp_df.loc[each_row_index, '商品名稱'])
                            _content = re.sub(r'《青葉臺菜X神老師推薦》\s{0,4}冰箱', '《神老師推薦》冰箱', _temp_df.loc[each_row_index, '商品名稱'])
                            # print(f'_content after  {_content}')
                            # 2021.01.15 >> 針對 《青葉臺菜X神老師推薦》被改成 《神老師推薦》 做特別處理
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = _temp_df.loc[each_row_index, '購買數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '單價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '備註']
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = to_get_subcontent(_temp_df.loc[each_row_index, '商品名稱'])

                            if '青葉臺菜' in _subcontent:
                                # (到貨日:花膠佛跳牆1組+鰻魚櫻花蝦米糕1組_2/3-2/9到貨)
                                # (到貨日:花膠佛跳牆1組+鰻魚櫻花蝦米糕1組_1/27-2/2到貨)
                                # (到貨日:1/27-2/2到貨)
                                nian_cai_pattern = r'[(]到貨日:.*[)]$'
                                _target_string = re.findall(nian_cai_pattern, _subcontent)
                                if len(_target_string):
                                    # found somthing
                                    _target_string = _target_string[0][5:-1]
                                    if len(_target_string) > 13:
                                        # something like 花膠佛跳牆1組+鰻魚櫻花蝦米糕1組_2/3-2/9到貨
                                        _subcontent = _target_string

                            # print(f'after to_get_subcontent: {_subcontent}')
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])              
                return is_found, is_error, exception_files


        elif platform == 'MOMO':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            # print('不是momo去識別化後的訂單')
                            for each_row_index in range(_temp_df.shape[0]):
                                try:
                                    _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                                except Exception as e:
                                    print(e)
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
                                _vendor = self.who_is_vendor_from_this_product(_content)
                                _how_many = _temp_df.loc[each_row_index, '數量']
                                _how_much = _temp_df.loc[each_row_index, '進價(含稅)'].astype(int)
                                _remark = ''
                                _room_temperature_shipping_id = ''
                                _low_temperature_shipping_id = ''
                                _last_charged_date = ''
                                _charged = False
                                _ifsend = False
                                _ifcancel = False
                                _subcontent = _temp_df.loc[each_row_index, '單品詳細']
                                pattern = r'\d{1,2}/\d{1,2}\s{0,1}-\s{0,1}\d{1,2}/\d{1,2}'
                                
                                if _subcontent == '無':
                                    _subcontent = _content

                                # 若「單品詳細」只有日期，則將與品名合在一起
                                if len(re.findall(pattern, _subcontent)):
                                    if re.findall(pattern, _subcontent)[0] == _subcontent:
                                        _subcontent = \
                                            self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                                  _temp_df.loc[each_row_index, '單品詳細']],
                                                                  ' - ')

                                #if len(re.findall(r'\d+/\d+/s{0,1}[-~]/s{0,1}\d+/\d+', _subcontent))
                                _room_temperature_shipping_link = ''
                                _low_temperature_shipping_link = ''
                                # 寫入資料
                                self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                        _file_created_date,
                                                                                        None,
                                                                                        None,
                                                                                        _txn_id,
                                                                                        _customer_name,
                                                                                        _receiver_name,
                                                                                        _paid_after_receiving,
                                                                                        _receiver_address,
                                                                                        _receiver_phone_nbr,
                                                                                        _receiver_mobile,
                                                                                        _content,
                                                                                        _how_many,
                                                                                        _how_much,
                                                                                        _remark,
                                                                                        _room_temperature_shipping_id,
                                                                                        _low_temperature_shipping_id,
                                                                                        _last_charged_date,
                                                                                        _charged,
                                                                                        _ifsend,
                                                                                        _ifcancel,
                                                                                        _vendor,
                                                                                        _subcontent,
                                                                                        _room_temperature_shipping_link,
                                                                                        _low_temperature_shipping_link]
                        else:
                            # 是momo去識別化後的訂單
                            _temp_df['貨運公司\n出貨地址'] = _temp_df['貨運公司\n出貨地址'].apply(lambda x: '(貨運公司出貨地址) ' +  x.replace('新竹貨運\n', ''))
                            #print('是momo去識別化後的訂單')
                            for each_row_index in range(_temp_df.shape[0]):
                                try:
                                    _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                                except Exception as e:
                                    print(e)
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
                                _vendor = self.who_is_vendor_from_this_product(_content)
                                _how_many = _temp_df.loc[each_row_index, '數量']
                                try:
                                    _how_much = _temp_df.loc[each_row_index, '進價(含稅)'].astype(int)
                                    # 2020.10.19 MOMO has removed this column.
                                except:
                                    pass
                                _remark = ''
                                _room_temperature_shipping_id = ''
                                _low_temperature_shipping_id = ''
                                _last_charged_date = ''
                                _charged = False
                                _ifsend = False
                                _ifcancel = False
                                _subcontent = _temp_df.loc[each_row_index, '單品詳細']
                                pattern = r'\d{1,2}/\d{1,2}\s{0,1}-\s{0,1}\d{1,2}/\d{1,2}'
                                if _subcontent == '無':
                                    _subcontent = _content

                                # 若「單品詳細」只有日期，則將與品名合在一起
                                if len(re.findall(pattern, _subcontent)):
                                    if re.findall(pattern, _subcontent)[0] == _subcontent:
                                        _subcontent = \
                                            self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                                  _temp_df.loc[each_row_index, '單品詳細']],
                                                                  ' - ')
                                                                  
                                _room_temperature_shipping_link = ''
                                _low_temperature_shipping_link = ''
                                # 寫入資料
                                self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                        _file_created_date,
                                                                                        None,
                                                                                        None,
                                                                                        _txn_id,
                                                                                        _customer_name,
                                                                                        _receiver_name,
                                                                                        _paid_after_receiving,
                                                                                        _receiver_address,
                                                                                        _receiver_phone_nbr,
                                                                                        _receiver_mobile,
                                                                                        _content,
                                                                                        _how_many,
                                                                                        _how_much,
                                                                                        _remark,
                                                                                        _room_temperature_shipping_id,
                                                                                        _low_temperature_shipping_id,
                                                                                        _last_charged_date,
                                                                                        _charged,
                                                                                        _ifsend,
                                                                                        _ifcancel,
                                                                                        _vendor,
                                                                                        _subcontent,
                                                                                        _room_temperature_shipping_link,
                                                                                        _low_temperature_shipping_link]

                    except Exception as e:
                        print(f"Integrating with MOMO {e}")
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])       
                return is_found, is_error, exception_files


        elif platform == 'Yahoo購物中心':

            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                            except Exception as e:
                                print(e)
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
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = _temp_df.loc[each_row_index, '數量']
                            # _how_much = _temp_df.loc[each_row_index, '商品成本'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '成本小計'].astype(int)  # Jerry堅持要以總額來做紀錄  20.09.29
                            _remark = _temp_df.loc[each_row_index, '購物車備註']
                            print(f"_remark: {_remark}")
                            # _remark = ''
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == '東森得易購':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            try:
                                _txn_id = self.try_to_be_int_in_str(
                                    _temp_df.loc[each_row_index, '訂單號碼'].astype(str) + '_' + _temp_df.loc[each_row_index, '訂單項次'].astype(str)
                                    )
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單號碼'].astype(str) + '_' + _temp_df.loc[each_row_index, '訂單項次'].astype(str)
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
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '成本'].astype(int)
                            _remark = ''
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = self._combine_columns([_temp_df.loc[each_row_index, '商品名稱'],
                                                            _temp_df.loc[each_row_index, '顏色'],
                                                            _temp_df.loc[each_row_index, '款式']],
                                                            ', ', only_meaningful=True)
                            # 根據20.08.21曉箐的說法，顏色跟款式不會同時有內容在裡面，而且這兩個欄位裡面樂天派不會放進空格。
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == '亞伯':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                print('Found Yabo.')
                print(txn_paths)
                try:
                    for txn_path in txn_paths:
                        _file_created_date = self._get_file_created_date(txn_path)
                        print('Yabo created_date', _file_created_date)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        print(f"亞伯 _temp_df:  {_temp_df.shape}")
                        # 亞伯有兩個版本，目前這個是PoDetail版
                        if 'PoDetail' in txn_path:
                            for each_row_index in range(_temp_df.shape[0]):
                                try:
                                    _txn_id = self._combine_columns([self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '廠商訂單編號']),
                                                                    self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '會員訂單編號'])],
                                                                    '-')
                                except Exception as e:
                                    print(e)
                                    _txn_id = self._combine_columns([_temp_df.loc[each_row_index, '廠商訂單編號'],
                                                                    _temp_df.loc[each_row_index, '會員訂單編號']],
                                                                    '-')
                                print('_txn_id', _txn_id)
                                _customer_name = _temp_df.loc[each_row_index, '消費者']
                                _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                                _paid_after_receiving = False
                                _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                                try:
                                    _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人連絡電話']
                                    _receiver_mobile = _temp_df.loc[each_row_index, '收貨人連絡電話']
                                    print(f"_receiver_phone_nbr1 {_receiver_phone_nbr}")
                                except:
                                    _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人聯絡電話']
                                    _receiver_mobile = _temp_df.loc[each_row_index, '收貨人聯絡電話']
                                    print(f"_receiver_phone_nbr2 {_receiver_phone_nbr}")
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                                _temp_df.loc[each_row_index, '選購規格']],
                                                                ', ')
                                _vendor = self.who_is_vendor_from_this_product(_content)
                                _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                                _how_much = _temp_df.loc[each_row_index, '成本小計'].astype(int)
                                _remark = ''
                                _room_temperature_shipping_id = ''
                                _low_temperature_shipping_id = ''
                                _last_charged_date = ''
                                _charged = False
                                _ifsend = False
                                _ifcancel = False
                                if not pd.isnull(_temp_df.loc[each_row_index, '選購規格']):
                                    _subcontent = _temp_df.loc[each_row_index, '選購規格']
                                else:
                                    _subcontent = _temp_df.loc[each_row_index, '品名']
                                
                                _room_temperature_shipping_link = ''
                                _low_temperature_shipping_link = ''
                                # 寫入資料
                                self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                        _file_created_date,
                                                                                        None,
                                                                                        None,
                                                                                        _txn_id,
                                                                                        _customer_name,
                                                                                        _receiver_name,
                                                                                        _paid_after_receiving,
                                                                                        _receiver_address,
                                                                                        _receiver_phone_nbr,
                                                                                        _receiver_mobile,
                                                                                        _content,
                                                                                        _how_many,
                                                                                        _how_much,
                                                                                        _remark,
                                                                                        _room_temperature_shipping_id,
                                                                                        _low_temperature_shipping_id,
                                                                                        _last_charged_date,
                                                                                        _charged,
                                                                                        _ifsend,
                                                                                        _ifcancel,
                                                                                        _vendor,
                                                                                        _subcontent,
                                                                                        _room_temperature_shipping_link,
                                                                                        _low_temperature_shipping_link]
                        elif 'shipmentReport' in txn_path:
                            print("shipmentReport in txn_path")
                            for each_row_index in range(_temp_df.shape[0]):
                                try:
                                    _txn_id = self._combine_columns([self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '廠商訂單編號']),
                                                                    self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '會員訂單編號'])],
                                                                    '-')
                                    print(f"_txn_id: {_txn_id}")
                                except Exception as e:
                                    print(e)
                                    _txn_id = self._combine_columns([_temp_df.loc[each_row_index, '廠商訂單編號'],
                                                                    _temp_df.loc[each_row_index, '會員訂單編號']],
                                                                    '-')
                                    print(f"_txn_id2: {_txn_id}")
                                _customer_name = _temp_df.loc[each_row_index, '消費者']
                                print(f"消費者: {_customer_name}")
                                _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                                print(f"收貨人姓名: {_receiver_name}")
                                _paid_after_receiving = False
                                _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                                print(f"收貨人地址: {_receiver_address}")
                                print("SO FAR SO GOOD!!")
                                # _receiver_phone_nbr = ''
                                # _receiver_mobile = ''
                                try:
                                    _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人連絡電話']
                                    _receiver_mobile = _temp_df.loc[each_row_index, '收貨人連絡電話']
                                #    print(f"_receiver_phone_nbr1 {_receiver_phone_nbr}")
                                except:
                                    _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人聯絡電話']
                                    _receiver_mobile = _temp_df.loc[each_row_index, '收貨人聯絡電話']
                                #     print(f"_receiver_phone_nbr2 {_receiver_phone_nbr}")
                                _content = self._combine_columns([_temp_df.loc[each_row_index, '品名'],
                                                                _temp_df.loc[each_row_index, '選購規格']],
                                                                ', ')
                                _vendor = self.who_is_vendor_from_this_product(_content)
                                _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                                _how_much = _temp_df.loc[each_row_index, '成本小計'].astype(int)
                                _remark = ''
                                _room_temperature_shipping_id = ''
                                _low_temperature_shipping_id = ''
                                _last_charged_date = ''
                                _charged = False
                                _ifsend = False
                                _ifcancel = False
                                if not pd.isnull(_temp_df.loc[each_row_index, '選購規格']):
                                    _subcontent = _temp_df.loc[each_row_index, '選購規格']
                                else:
                                    _subcontent = _temp_df.loc[each_row_index, '品名']
                                
                                _room_temperature_shipping_link = ''
                                _low_temperature_shipping_link = ''
                                # 寫入資料
                                self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                        _file_created_date,
                                                                                        None,
                                                                                        None,
                                                                                        _txn_id,
                                                                                        _customer_name,
                                                                                        _receiver_name,
                                                                                        _paid_after_receiving,
                                                                                        _receiver_address,
                                                                                        _receiver_phone_nbr,
                                                                                        _receiver_mobile,
                                                                                        _content,
                                                                                        _how_many,
                                                                                        _how_much,
                                                                                        _remark,
                                                                                        _room_temperature_shipping_id,
                                                                                        _low_temperature_shipping_id,
                                                                                        _last_charged_date,
                                                                                        _charged,
                                                                                        _ifsend,
                                                                                        _ifcancel,
                                                                                        _vendor,
                                                                                        _subcontent,
                                                                                        _room_temperature_shipping_link,
                                                                                        _low_temperature_shipping_link]
                except Exception as e:
                    print(e)
                    is_error = True
                    exception_files.append(ntpath.split(txn_path)[1])                
            return is_found, is_error, exception_files


        elif platform == 'UDN':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '訂購人姓名']
                            _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人手機']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '進貨價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '備註/卡片內容']
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == '台塑':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '訂購人姓名']
                            _receiver_name = _temp_df.loc[each_row_index, '收貨人姓名']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收貨人地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收貨人手機']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            try:
                                _how_much = _temp_df.loc[each_row_index, '成本價'].astype(int)
                            except Exception as e:
                                print(e)
                                _how_much = _temp_df.loc[each_row_index, '進貨價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '備註/卡片內容']
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == 'LaNew':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '收件人'] # 因為沒有客戶(購買者)欄位，故以收件人取代
                            _receiver_name = _temp_df.loc[each_row_index, '收件人']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收件人電話']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '品名']
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_many = _temp_df.loc[each_row_index, '數量'].astype(int)
                            _how_much = _temp_df.loc[each_row_index, '單價'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '商品備註']
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _content
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == 'Friday':

            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            #print(txn_path, each_row_index)
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'][1:])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號'][1:]
                            _customer_name = _temp_df.loc[each_row_index, '收件人'][1:]
                            _receiver_name = _temp_df.loc[each_row_index, '收件人'][1:]
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '收件人地址'][1:]
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '收件人手機'][1:]
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '商品名稱'][1:]
                            _vendor = self.who_is_vendor_from_this_product(_content)
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
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱'][1:]
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files

        
        elif platform == '龍哥':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                # 建立一個檢查是否規格為 XXXX*\d的機制
                def check_if_contains_amounts_in_content_list(targets_list):
                    pattern = r'\S*[*]\d+'
                    results = list()
                    for each_target in targets_list:
                        # 檢查是否找到對應的規則
                        results.append(
                            len(re.findall(pattern, each_target)) > 0
                        )
                    return all(results)


                for txn_path in txn_paths:
                    file_name_without_ext = ntpath.split(txn_path)[1]
                    vendor = file_name_without_ext.split('_')[-1].split('.')[0]
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_excel(txn_path))
                        if '買家備註' not in _temp_df.columns:
                            _temp_df.loc[:, '買家備註'] = None
                        if '賣家備註' not in _temp_df.columns:
                            _temp_df.loc[:, '賣家備註'] = None

                        # _temp_df['訂單編號'] = _temp_df['訂單編號'].astype(str)
                        unique_txn_ids = list(_temp_df['訂單編號'].unique())
                        # print(f'unique_txn_ids len: {len(unique_txn_ids)}')
                        for each_unique_txn_id in unique_txn_ids:
                            tdf = _temp_df[_temp_df['訂單編號']==each_unique_txn_id].reset_index(drop=True)
                            _txn_id = each_unique_txn_id
                            _customer_name = tdf.loc[0, '收件人']
                            _receiver_name = _customer_name
                            _paid_after_receiving = False
                            _receiver_address = tdf.loc[0, '地址']
                            _receiver_phone_nbr = tdf.loc[0, '收件人電話']
                            _receiver_mobile = _receiver_phone_nbr
                            if '商品名稱' in _temp_df.columns:
                                _content = tdf.loc[0, '商品名稱']
                            else:
                                _content = tdf.loc[0, '商品規格']
                            _vendor = vendor
                            # print(f"_how_much: {tdf.loc[:, '商品單價'].astype(int)}  {tdf.loc[:, '數量'].astype(int)} {tdf.loc[:, '商品單價'].astype(int) * tdf.loc[:, '數量'].astype(int)}")
                            _how_much = sum(tdf.loc[:, '商品單價'].astype(int) * tdf.loc[:, '數量'].astype(int))
                            # print(f"_how_much: {_how_much}")
                            _how_many = 1

                            if pd.isnull(tdf.loc[0, "買家備註"]) and pd.isnull(tdf.loc[0, "賣家備註"]):
                                _remark = ''
                            elif pd.isnull(tdf.loc[0, "買家備註"]) == False and pd.isnull(tdf.loc[0, "賣家備註"]) == False:
                                _remark = \
                                    f'買家: {tdf.loc[0, "買家備註"]}\n賣家: {tdf.loc[0, "賣家備註"]}'
                            elif pd.isnull(tdf.loc[0, "買家備註"]) == False:
                                _remark = f'買家: {tdf.loc[0, "買家備註"]}'
                            elif pd.isnull(tdf.loc[0, "賣家備註"]) == False:
                                _remark = f'賣家: {tdf.loc[0, "賣家備註"]}'

                            if '貨運編號' in tdf.columns:
                                _room_temperature_shipping_id = tdf.loc[0, '貨運編號']
                            else:
                                _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            # if check_if_contains_amounts_in_content_list(tdf.loc[:, '商品規格'].tolist()):
                            #     # 如果已經含有數量了，就不需要再次餵進去
                            #     _subcontent = ', '.join(tdf.loc[:, '商品規格'])
                            # else:
                            #     _subcontent = \
                            #         ', '.join((tdf.loc[:, '商品規格'] + '*' + tdf.loc[:, '數量'].astype(str)).tolist())
                            _subcontent = list()
                            prods = tdf.loc[:, '商品規格'].tolist()
                            nums = tdf.loc[:, '數量'].tolist()
                            for i, j in zip(prods, nums):
                                _subcontent.append(
                                    self.multiply_products(i, int(j))
                                )
                            
                            _subcontent = self.aggregate_elements_in_subcontent(", ".join(_subcontent))
                            
                            sum_how_many = sum(tdf.loc[:, '數量'].astype(int))
                            if sum_how_many >= 4 and _vendor in ['水根肉乾', '水根']:
                                _subcontent = _subcontent + ', 袋子*' + str(int(sum_how_many/4))

                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''

                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1]) 
                #self.aggregated_txns.to_excel('long.xlsx', index=False)              
                return is_found, is_error, exception_files

        
        elif platform == '快車肉乾銷港':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
                is_found = False
                return is_found, is_error, exception_files
            else:
                for txn_path in txn_paths:
                    try:
                        _file_created_date = self._get_file_created_date(txn_path)
                        _temp_df = self._clean_dataframe(pd.read_csv(txn_path))
                        _temp_df = _temp_df[_temp_df['Payment Status'] == 'paid'].reset_index(drop=True)

                        _temp_df.loc[:, '_temp_subcontent'] = _temp_df['Product Name'].apply(lambda x: x.split('】')[-1].split('-')[-1].strip() if '-' in x else x.split('】')[-1].strip())
                        _temp_df.loc[:, 'customer_name'] = _temp_df['First Name'] + ' ' + _temp_df['Last Name']
                        _temp_df.loc[:, '_temp_remark'] = _temp_df['Order Note'] + '; email:' + _temp_df['Buyer\'s Email Address']
                        _temp_df['Shipping Address'] = _temp_df['Shipping Address'].apply(lambda x: x.replace('\n', ' '))
                        for each_row_index in range(_temp_df.shape[0]):
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, 'Order#'])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, 'Order#']
                            _customer_name = _temp_df.loc[each_row_index, 'customer_name']
                            _receiver_name = _temp_df.loc[each_row_index, 'customer_name']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, 'Shipping Address']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, 'Buyer\'s Contact Number']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, 'Product Name']
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_much = _temp_df.loc[each_row_index, 'Product Price'].astype(int)
                            _how_many = _temp_df.loc[each_row_index, 'Quantity Ordered'].astype(int)
                            _remark = _temp_df.loc[each_row_index, '_temp_remark']
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '_temp_subcontent']
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print('Alicia Integrating', platform, e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files
        
        
        elif platform == '博客來':
            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                            try:
                                _txn_id = self.try_to_be_int_in_str(_temp_df.loc[each_row_index, '訂單編號'])
                            except Exception as e:
                                print(e)
                                _txn_id = _temp_df.loc[each_row_index, '訂單編號']
                            _customer_name = _temp_df.loc[each_row_index, '收件人']
                            _receiver_name = _temp_df.loc[each_row_index, '收件人']
                            _paid_after_receiving = False
                            _receiver_address = _temp_df.loc[each_row_index, '配送地址']
                            _receiver_phone_nbr = _temp_df.loc[each_row_index, '聯絡電話']
                            _receiver_mobile = _receiver_phone_nbr
                            _content = _temp_df.loc[each_row_index, '商品名稱']
                            _vendor = self.who_is_vendor_from_this_product(_content)
                            _how_much = _temp_df.loc[each_row_index, '進貨價'].astype(int)
                            _how_many = _temp_df.loc[each_row_index, '訂購量'].astype(int)
                            _remark = ''
                            _room_temperature_shipping_id = ''
                            _low_temperature_shipping_id = ''
                            _last_charged_date = ''
                            _charged = False
                            _ifsend = False
                            _ifcancel = False
                            _subcontent = _temp_df.loc[each_row_index, '商品名稱']
                            _room_temperature_shipping_link = ''
                            _low_temperature_shipping_link = ''
                            # 寫入資料
                            self.aggregated_txns.loc[self.aggregated_txns.shape[0]] = [platform,
                                                                                    _file_created_date,
                                                                                    None,
                                                                                    None,
                                                                                    _txn_id,
                                                                                    _customer_name,
                                                                                    _receiver_name,
                                                                                    _paid_after_receiving,
                                                                                    _receiver_address,
                                                                                    _receiver_phone_nbr,
                                                                                    _receiver_mobile,
                                                                                    _content,
                                                                                    _how_many,
                                                                                    _how_much,
                                                                                    _remark,
                                                                                    _room_temperature_shipping_id,
                                                                                    _low_temperature_shipping_id,
                                                                                    _last_charged_date,
                                                                                    _charged,
                                                                                    _ifsend,
                                                                                    _ifcancel,
                                                                                    _vendor,
                                                                                    _subcontent,
                                                                                    _room_temperature_shipping_link,
                                                                                    _low_temperature_shipping_link]
                    except Exception as e:
                        print(e)
                        is_error = True
                        exception_files.append(ntpath.split(txn_path)[1])                
                return is_found, is_error, exception_files


        elif platform == '整合檔':
            # 整合檔跟其他平台最大的差別在於：它是可以直接被整合進資料庫裡的。
            # 因此不需要多做甚麼資料整理，但仍然要清一下各個column，免得有多餘的空白或跳行。

            if len(txn_paths) == 0:
                # print('未找到任何來自『' + platform + '』的交易資料。')
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
                        _temp_df['常溫宅單編號'][~pd.isnull(_temp_df['常溫宅單編號'])] = _temp_df['常溫宅單編號'][~pd.isnull(_temp_df['常溫宅單編號'])].apply(lambda x: str(x).split('.')[0].replace('\'', '').replace('-', ''))
                        _temp_df['低溫宅單編號'][~pd.isnull(_temp_df['低溫宅單編號'])] = _temp_df['低溫宅單編號'][~pd.isnull(_temp_df['低溫宅單編號'])].apply(lambda x: str(x).split('.')[0].replace('\'', '').replace('-', ''))
                        _temp_df['貨到付款'][pd.isnull(_temp_df['貨到付款'])] = False
                        #_temp_df['修訂出貨日'] = pd.to_datetime(_temp_df['修訂出貨日'])
                        #_temp_df['修訂出貨日'][pd.isnull(_temp_df['修訂出貨日'])] = ''
                        _temp_df['地址'][pd.isnull(_temp_df['地址'])] = ''
                        _temp_df['地址'][pd.isnull(_temp_df['地址'])] = ''
                        _temp_df['金額'][pd.isnull(_temp_df['金額'])] = 0
                        _temp_df['數量'][pd.isnull(_temp_df['數量'])] = 1
                        _temp_df['已寄出'][pd.isnull(_temp_df['已寄出'])] = False
                        _temp_df['已取消'][pd.isnull(_temp_df['已取消'])] = False
                        _temp_df['規格'][pd.isnull(_temp_df['規格'])] = _temp_df['內容物'][pd.isnull(_temp_df['規格'])]
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


    def _integrate_all_platforms(self):
        platforms_found, platforms_not_found, exception_files = [], [], []
        # 整合所有訂單後, 回傳有找到的平台跟沒有找到的, 以及有問題的檔案們
        for each_platform in self.platforms:
            # print('_integrate_all_platforms b4', each_platform, self.aggregated_txns)
            is_found, _is_error, sub_exception_files = self._integrate_with(each_platform)
            # print('_integrate_all_platforms after', is_found, _is_error, sub_exception_files, self.aggregated_txns)
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
        return self.force_float_to_be_int_and_to_string(target)

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

    def multiply_products(self, target_string, multiplier, split_by='+'):
        '''
        target_string 長的類似「abc*1x, ccd*12x, abc*3x, ccd*1g」or「牛湯4+牛渣渣3」or「炸醬」，
        multiplier則是其數量，目的為：multiply_products("a*1 + b*2", 3) >> "a*3 + b*6"
        '''
        prod_list = list()
        num_list = list()
        pattern = re.compile(r'\d+$')

        for each_prod in target_string.split(split_by):
            each_prod = each_prod.strip()
            if len(re.findall(pattern, each_prod)) > 0:
                num_part = int(re.findall(pattern, each_prod)[0])
                prod_part = each_prod[:len(each_prod)-len(str(num_part))]
                num_list.append(num_part)
                prod_list.append(prod_part)
            else:
                num_list.append(1)
                prod_list.append(each_prod)
        
        _temp = list()
        for i, j in zip(prod_list, num_list):
            _temp.append(f"{i}*{j*multiplier}")
        
        return ", ".join(_temp)

    def aggregate_elements_in_subcontent(self, target_string):
        # 這個函式用來將同一個「自訂訂單編號」中相同的品項合併
        # 這個函式理論上收到「abc*1x, ccd*12x, abc*3x, ccd*1g」後，應該產出:
        # 「abc*4x, ccd*12x, ccd*1g」
        # 將前綴詞與產品分離出來
        prefix_words = ' '.join([_ for _ in target_string.split() if '*' not in _])
        splitted_target_string = target_string.split(',')

        pattern = r'\S+[*]\d+\S*'
        ## 接著我們組一個大字典，分別將all_found以下列方式儲存:
        ## {product_name: [商品名稱], volume: [數量], unit: [量詞, 沒有的話填入空字串], spec: [商品名稱-量詞]}
        ## 最後一個spec是為了抓出可以整合的商品
        subcontent_dict = {'product_name': list(), 'volume': list(), 'unit': list(), 'spec': list()}
        
        for each_splitted_element in splitted_target_string:
            # 先判斷裡面的每一個元素是否符合r'\S+[*]\d+\S*'的規則
            mapping = re.search(pattern, each_splitted_element)
            if mapping is not None:
                mapping_words = mapping.group()

                product_name = mapping_words.split('*')[0]
                volume = re.search(r'\d+', mapping_words.split('*')[1]).group()
                unit = mapping_words.split('*')[1].replace(volume, '')
                # 若沒有量詞時，這樣會自動給值空字串
                subcontent_dict['product_name'].append(product_name)
                subcontent_dict['volume'].append(volume)
                subcontent_dict['unit'].append(unit)
                subcontent_dict['spec'].append(product_name + '-' + unit)

        ## 接著開始把有同樣product_name以及同樣unit的組合加起來
        temp_df = pd.DataFrame(data=subcontent_dict)
        temp_df.volume = temp_df.volume.astype(int)
        
        '''    這個temp_df大概會長這樣
        .  product_name volume unit    spec
        0          abc      1    x   abc-x
        1          ccd     12    x   ccd-x
        2          abc      3    x   abc-x
        3          ccd      1    g   ccd-g
        4          xxc     99   ss  xxc-ss
        5          xxc      4   ss  xxc-ss
        '''
        temp_subcontent_in_list = list()

        for each_unique_spec in sorted(temp_df.spec.unique()):
            _tdf = temp_df[temp_df.spec==each_unique_spec]
            
            this_product_name = _tdf.product_name.tolist()[0]
            this_volume_sum = _tdf.volume.sum()
            this_unit = _tdf.unit.tolist()[0]
            temp_subcontent_in_list.append(
                this_product_name + '*' + str(this_volume_sum) + this_unit)
        
        return prefix_words + ' ' + ', '.join(temp_subcontent_in_list)

    def to_split_old_unique_ids(self, old_unique_id_in_list):
        # As old unique_id contains formats like channel-vendor1, vendor2-txn_id,
        # which is aborted now; We have to let Alicia know that means:
        # "channel-vendor1-txn_id" & "channel-vendor2-txn_id" these 2 unique_ids.
        _temp_list = list()
        for each_old_unique_id in old_unique_id_in_list:
            try:
                if ',' in each_old_unique_id:
                    channel, vendors_string, txn_id = each_old_unique_id.split('|')
                    if ', ' in vendors_string:
                        vendors_in_list = vendors_string.split(', ')
                    else:
                        vendors_in_list = vendors_string.split(',')
                    for each_vendor in vendors_in_list:
                        _temp_list.append(channel + '|' + each_vendor + '|' + txn_id)
                else:
                    _temp_list.append(each_old_unique_id)
            except Exception as e:
                print(f'to_split_old_unique_ids ERROR {e}')
        return _temp_list


if __name__ == '__main__':
    import re, pandas as pd
    x = '加購－寬麵條(數量:1),\n蔣老爹 小資女獨享 水餃2包組(商品規格:麻辣餃x1+四季豆x1)\n紅30年的雞-阿雪手撕雞4盒(小盒)(顏色:4盒)'
    ali = ALICIA()
    #print(ali.vendors)
    print(ali.who_is_vendor_from_this_product(x))
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
