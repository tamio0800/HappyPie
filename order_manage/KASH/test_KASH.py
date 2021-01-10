import unittest
from kashgari_final_with_Alicia import kashgari_model

class KASHGARI_TEST(unittest.TestCase):

    def setUp(self):
        self.kash = kashgari_model()

    def test_kash_could_initialize(self):
        target = '[["品蔬佛跳牆1組+百蔬宴米糕1組_1/27-2/2到貨","《青葉臺菜X神老師推薦》總鋪ㄟ花膠佛跳牆禮盒1組(2000g/組)(到貨日:2/2-2/9到貨)","品蔬佛跳牆1組+百蔬宴米糕1組_1/27-2/2到貨"\
            ,"花膠佛跳牆1組+鰻魚櫻花蝦米糕1組_2/3-2/9到貨"], \
            ["1","1","1","1"]]'
        target = eval(target)
        
        print(f'self.kash.get_results_alicia(target[0], target[1]) \
        {self.kash.get_results_alicia(target[0], target[1])}')

if __name__ == '__main__':
    unittest.main()

