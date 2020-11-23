import unittest
from Alicia_0611 import *
from time import time
#print('\n\n', os.path.abspath(os.curdir), '\n\n')

class TestALICIA(unittest.TestCase):
    
    def setUp(self):
        # 每一隻test執行前都會啟動
        self.alicia = ALICIA()
        self.where_does_orders_locate = 'order_manage/ALICIA/temp_files/'
        self.test_order_file_name = '20201119_export_default.xls'
        self.alicia.raw_txns_dir = self.where_does_orders_locate
        self.start_time = time()
        # print("Set up alicia")


    def tearDown(self):
        # 每一隻test執行後都會啟動
        self.time_diff = time() - self.start_time
        print(f'Test {self.id()} has spent: {round(self.time_diff, 2)} seconds.')


    def test_if_ALICIA_could_see_the_data(self):
        # 將訂單檔案放到給定的資料夾「self.where_does_orders_locate」，
        # 確認ALICIA能不能看到這些資料
        if_files_are_all_good, exception_files = \
            self.alicia.check_if_all_files_are_good_for_ALICIA_pipeline(
                os.listdir(self.where_does_orders_locate)
            )
        self.assertIn(self.test_order_file_name, os.listdir(self.where_does_orders_locate))
        # 先確認有看到要測試的資料
        self.assertEqual(if_files_are_all_good, True, f'Exception Files:\n{exception_files}')
        # 確認pipeline正確
        

    def test_if_ALICIA_could_read_the_data(self):
        # 給定要搜尋的路徑
        platforms_found, _platforms_not_found, _after_alicia_exception_files = \
            self.alicia._integrate_all_platforms()
        self.assertGreater(len(platforms_found), 0)
        self.assertIn('樂天派官網', platforms_found)
        # 檢查是否有找到符合標準的訂單資料
        self.assertGreater(self.alicia.aggregated_txns.shape[0], 0)
        # 檢查讀取到的內容
        

    def test_ALICIA_by_integrate_with_function_rather_than_integrate_all_platforms(self):
        # 先測試單一的「integrate_with」函式，以樂天派官網作為標的
        _is_found, _is_error, _exception_files = \
            self.alicia._integrate_with('樂天派官網')
        self.assertTrue(_is_found, f"_exception_files: {_exception_files}")

        # 確認每一個供應商欄位都只有一個值，而且不為0
        amount_of_vendor_list = self.alicia.aggregated_txns['供應商'].apply(
            lambda x: len(x.split(',')) if not pd.isnull(x) else 0
            ).tolist()
        max_amount_of_vendors, min_amount_of_vendors = max(amount_of_vendor_list), min(amount_of_vendor_list)
        self.assertTrue(all(_ == 1 for _ in amount_of_vendor_list),
        f'\nmax_num: {max_amount_of_vendors}\nmin_num: {min_amount_of_vendors}')


    def test_check_vendor_name(self):
        product_name = \
            '【神老師推薦】 冰箱不可或缺的鮮食備(任選5-6件組)(顏色:錵魚一夜干*2+挪威鯖魚*4)'
        vendor = self.alicia.who_is_vendor_from_this_product(product_name)
        self.assertEqual(vendor, '鮮綠生活', f'Parsed Vendor:  {vendor}')


    def test_vendor_column_should_only_contain_one_value(self):
        # 先測試這邊能不能讀到ALICIA在上一個測試儲存的資料 >> 不行!! 不能直接寫下去
        self.alicia._integrate_all_platforms()
        # 確認每一個供應商欄位都只有一個值，而且不為0
        amount_of_vendor_list = self.alicia.aggregated_txns['供應商'].apply(
            lambda x: len(x.split(',')) if not pd.isnull(x) else 0
            ).tolist()
        max_amount_of_vendors, min_amount_of_vendors = max(amount_of_vendor_list), min(amount_of_vendor_list)
        self.assertTrue(all(_ == 1 for _ in amount_of_vendor_list),
        f'\nmax_num: {max_amount_of_vendors}\nmin_num: {min_amount_of_vendors}')


    def test_vendor_column_should_only_contain_one_value_after_PRE_CLEAN_RAW_TXNS(self):
        # PRE_CLEAN_RAW_TXNS 的用意是製作unique_id，好讓資料庫可以分辨資料是否有重複
        self.alicia._integrate_all_platforms()
        self.alicia.pre_clean_raw_txns()

        amount_of_vendor_list = self.alicia.aggregated_txns['供應商'].apply(
            lambda x: len(x.split(',')) if not pd.isnull(x) else 0
            ).tolist()
        max_amount_of_vendors, min_amount_of_vendors = max(amount_of_vendor_list), min(amount_of_vendor_list)
        self.assertTrue(all(_ == 1 for _ in amount_of_vendor_list),
        f'\nmax_num: {max_amount_of_vendors}\nmin_num: {min_amount_of_vendors}')


    def test_only_one_vendor_after_combine_aggregated_txns_and_user_uploaded_aggregated_txns(self):
        # combine_aggregated_txns_and_user_uploaded_aggregated_txns
        # 上面這個函式用來將pre_clean_txn中【同訂單編號的交易們】合併在一起，變成一個row，
        # 我們要測試經過這個函式後，是否每一個row當中只會有一個vendor
        self.alicia._integrate_all_platforms()
        
        
        combined_df = self.alicia.combine_aggregated_txns_and_user_uploaded_aggregated_txns(
            self.alicia.aggregated_txns, None)
        
        amount_of_vendor_list = combined_df['供應商'].apply(
            lambda x: len(x.split(',')) if not pd.isnull(x) else 0
            ).tolist()
        max_amount_of_vendors, min_amount_of_vendors = max(amount_of_vendor_list), min(amount_of_vendor_list)
        self.assertTrue(all(_ == 1 for _ in amount_of_vendor_list),
        f'\nmax_num: {max_amount_of_vendors}\nmin_num: {min_amount_of_vendors}')


    '''def test_check_columns_after_TO_ONE_UNIQUE_ID_DF_AFTER_KASH(self):
        # 嘗試在這邊重構此一函式，將原來的txn_id分拆成:
        # txn_id >> 原始收到的txn_id長相，中文改名叫『原始訂單編號』
        # alicia_txn_id >> 經過【分拆供應商】後，我們給每一個row一個對應id based on txn_id，中文名叫『Alicia訂單編號』
        
        self.alicia._integrate_all_platforms()
        print('Before combine shape: ', self.alicia.aggregated_txns.shape)
        self.alicia.pre_clean_raw_txns()
        
        dataframe_after_parsing = \
            self.alicia.to_one_unique_id_df_after_kash(self.alicia.aggregated_txns)
        print('dataframe_after_parsing shape: ', dataframe_after_parsing.shape)

        # 首先測試裡面應該有 "txn_id(原始訂單編號)", "alicia_txn_id(Alicia訂單編號)"欄位
        # self.assertIn('原始訂單編號', dataframe_after_parsing.columns)
        # self.assertIn('Alicia訂單編號', dataframe_after_parsing.columns)
        self.assertIn('供應商', dataframe_after_parsing.columns)'''

    
    def test_each_alicia_txn_id_mapping_to_one_vendor_after_TO_ONE_UNIQUE_ID_DF_AFTER_KASH(self):
        # 測試unique(txn_id + vendor).count()在餵入此函示前後一致
        
        self.alicia._integrate_all_platforms()
        self.alicia.pre_clean_raw_txns()
        befor_txn_id_vendor_codes = \
            self.alicia.aggregated_txns['訂單編號'] + '-' + self.alicia.aggregated_txns['供應商']

        dataframe_after_parsing = \
            self.alicia.to_one_unique_id_df_after_kash(self.alicia.aggregated_txns)

        self.alicia.aggregated_txns.to_excel('order_manage/ALICIA/dataframe_before_parsing.xlsx', index=False)
        dataframe_after_parsing.to_excel('order_manage/ALICIA/dataframe_after_parsing.xlsx', index=False)

        after_txn_id_vendor_codes = \
            dataframe_after_parsing['訂單編號'] + '-' + dataframe_after_parsing['供應商']
        
        self.assertEqual(len(set(befor_txn_id_vendor_codes)),
        len(set(after_txn_id_vendor_codes)))


    def test_if_2_shipping_id_columns_in_dataframe(self):
        # 測試 常溫宅單編號、低溫宅單編號、常溫貨運連結、低溫貨運連結 兩個欄位，是否有在aggregated_txns裡
        # 常溫宅單編號  >>  room_temperature_shipping_id
        # 低溫宅單編號  >>  low_temperature_shipping_id
        # 常溫貨運連結  >>  room_temperature_shipping_link
        # 低溫貨運連結  >>  low_temperature_shipping_link
        self.assertIn('常溫宅單編號', self.alicia.aggregated_txns.columns)
        self.assertIn('低溫宅單編號', self.alicia.aggregated_txns.columns)
        self.assertIn('常溫貨運連結', self.alicia.aggregated_txns.columns)
        self.assertIn('低溫貨運連結', self.alicia.aggregated_txns.columns)

    def test_aggregate_elements_in_subcontent_function(self):
        # 用來測試將『同一個「自訂訂單編號」中相同的品項合併』的函式
        # 這個函式理論上收到「abc*1x, ccd*12x, abc*3x, ccd*1g」後，應該產出:
        # 「abc*4x, ccd*12x, ccd*1g」
        test_string = 'abc*1x, ccd*12x, abc*3x, ccd*1g, xxc*99ss, xxc*4ss'
        real_aggregated_txn_in_list = ['abc*4x', 'ccd*12x', 'ccd*1g', 'xxc*103ss']
        calc_aggregated_txn = self.alicia.aggregate_elements_in_subcontent(test_string)
        self.assertTrue(
            all([_.strip() in real_aggregated_txn_in_list for _ in calc_aggregated_txn.split(',')]),
            calc_aggregated_txn
        )

    def test_to_split_old_unique_ids(self):
        df = pd.read_excel('order_manage/ALICIA/20201123-012954_待處理訂單資料整合檔.xlsx')
        vendor_series = \
            df['內容物'].apply(self.alicia.who_is_vendor_from_this_product)
        df.loc[:, 'vendor'] = vendor_series
        df.to_excel('vendor_p.xlsx', index=False)
        self.assertTrue(True)
        

        


if __name__ == '__main__':
    unittest.main()