import unittest
from Alicia_0611 import *


class TestALICIA(unittest.TestCase):
    
    def setUp(self):
        self.alicia = ALICIA()
        self.where_does_orders_locate = 'order_manage/ALICIA/temp_files/'
        self.test_order_file_name = '20201119_export_default (1).xls'
        self.alicia.raw_txns_dir = self.where_does_orders_locate
        print("Set up alicia")

    def tearDown(self):
        print("Done test.")

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
        platforms_found, platforms_not_found, after_alicia_exception_files = \
            self.alicia._integrate_all_platforms()
        self.assertGreater(len(platforms_found), 0)
        self.assertIn('樂天派官網', platforms_found)
        # 檢查是否有找到符合標準的訂單資料
        self.assertGreater(self.alicia.aggregated_txns.shape[0], 0)
        # 檢查讀取到的內容
        self.alicia.aggregated_txns.to_excel('order_manage/ALICIA/test_temp_excel.xlsx')

    def test_ALICIA_by_integrate_with_function_rather_than_integrate_all_platforms(self):
        # 先測試單一的「integrate_with」函式，以樂天派官網作為標的
        self.alicia._integrate_with


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






if __name__ == '__main__':
    unittest.main()