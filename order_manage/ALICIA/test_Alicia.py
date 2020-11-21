import unittest
from Alicia_0611 import *


class TestALICIA(unittest.TestCase):
    
    def setUp(self):
        self.alicia = ALICIA()
        self.where_does_orders_locate = 'order_manage/ALICIA/temp_files/'
        print("Set up alicia")

    def tearDown(self):
        print("Done test.")

    def test_if_ALICIA_could_read_the_data(self):
        if_files_are_all_good, exception_files = \
            self.alicia.check_if_all_files_are_good_for_ALICIA_pipeline(
                os.listdir(self.where_does_orders_locate)
            )
        self.assertEqual(if_files_are_all_good, True, 
        f'Exception Files:\n{exception_files}')


if __name__ == '__main__':
    unittest.main()