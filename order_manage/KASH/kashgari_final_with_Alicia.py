import pandas as pd, numpy as np
import kashgari
import os, sys, re
from time import time, sleep
import tensorflow as tf
from cleansing import *


class kashgari_model:

    def __init__(self):
        # '/mnt/c/Users/common tata/Desktop/Edony_AI/004_Customers/006_E-Commerce/20200331_Tina_aggregation/079_website/happypi_annie/20200426_bert/logs'
        # /home/edony/happypi_0610_annie
        self.model = kashgari.utils.load_model(os.path.join(os.curdir, '20200426_bert/logs'))
        self.graph = tf.get_default_graph()
        # 下面這行是為了讓模型先跑一次
        self.model.predict(self.preclean_seq('無敵美味大漢堡'))


    def preclean_seq(self, target_strs):

        if type(target_strs) is str:
            return [[_ for _ in target_strs],]

        elif type(target_strs) is list:
            _temp = []
            for each_target_str in target_strs:
                _temp.append([_ for _ in each_target_str])
            return _temp


    def get_annotations(self, target_str):
        _input = self.preclean_seq(target_str)
        print('get_annotations 1: ', _input, target_str)
        with self.graph.as_default():
            _output = self.model.predict(_input)[0]
            print('get_annotations 1.1: ', _output)
        prods_index, nums_index = self.get_prod_and_num_index(target_str, _output)

        print('get_annotations 2: ', prods_index, nums_index)
        try:
            opt_prods, opt_nums =  self.match(prods_index, nums_index, target_str)
        except:
            opt_prods, opt_nums = 'None', 'None'

        return opt_prods, opt_nums

        # prods = str(self.get_prod(target_str, _output)).replace('[', '').replace(']', '').replace('\'', '')
        # nums = str(self.get_qnty(target_str, _output)).replace('[', '').replace(']', '').replace('\'', '')
        # return prods + '||||' + nums


    def get_prod(self, y, y_):
        prods = []
        prod = ''
        for _y, _y_ in zip(y, y_):
            if _y_ in ['B-PROD', 'I-PROD']:
                prod +=  _y
            elif _y_ not in ['B-PROD', 'I-PROD'] and prod != '':
                prods.append(prod)
                prod = ''
        if prod != '':
            prods.append(prod)
        return prods

    def get_qnty(self, y, y_):
        qnties = []
        qnty = ''
        for _y, _y_ in zip(y, y_):
            if _y_ in ['B-QNT', 'I-QNT']:
                qnty +=  _y
            elif _y_ not in ['B-QNT', 'I-QNT'] and qnty != '':
                qnties.append(qnty)
                qnty = ''
        if qnty != '':
            qnties.append(qnty)
        return qnties

    def get_prod_and_num_index(self, target_str, model_output):
        # target_str, model_output = target, output
        _prods_index, prods_index = [], []
        _nums_index, nums_index = [], []

        prods, prod = [], ''
        qnties, qnty = [], ''

        for li_i, (_y, _y_) in enumerate(zip(target_str, model_output)):

            if _y_ in ['B-PROD', 'I-PROD'] and li_i != len(target_str) - 1:
                prod +=  _y
                _prods_index.append(li_i)

            elif _y_ in ['B-PROD', 'I-PROD'] and li_i == len(target_str) - 1:
                # 產品名稱在最尾處結束
                prod +=  _y
                _prods_index.append(li_i)
                prods.append(prod)
                prod = ''
                prods_index.append((min(_prods_index),))
                _prods_index = []

            elif _y_ not in ['B-PROD', 'I-PROD'] and prod != '':
                # 非在最尾處結束
                prods.append(prod)
                prods_index.append((min(_prods_index), max(_prods_index)))
                _prods_index = []
                prod = ''

            if _y_ in ['B-QNT', 'I-QNT'] and li_i != len(target_str) - 1:
                qnty +=  _y
                _nums_index.append(li_i)

            elif _y_ in ['B-QNT', 'I-QNT'] and li_i == len(target_str) - 1:
                # 數量在最尾處結束
                qnty +=  _y
                _nums_index.append(li_i)
                qnties.append(qnty)
                qnty = ''
                nums_index.append((min(_nums_index),))
                _nums_index =[]

            elif _y_ not in ['B-QNT', 'I-QNT'] and qnty != '':
                qnties.append(qnty)
                nums_index.append((min(_nums_index), max(_nums_index)))
                _nums_index = []
                qnty = ''

        return (prods_index, nums_index)


    def get_prod_and_num_index_alicia(self, target_str_list, model_output_list):
        # 相關註解請參照 get_prod_and_num_index
        # 這個函式只是 get_prod_and_num_index 的輸入跟輸出改成list而已
        prods_indexes, nums_indexes = [], []
        for target_str, model_output in zip(target_str_list, model_output_list):
            _prods_index, prods_index = [], []
            _nums_index, nums_index = [], []
            prods, prod = [], ''
            qnties, qnty = [], ''
            for li_i, (_y, _y_) in enumerate(zip(target_str, model_output)):
                if _y_ in ['B-PROD', 'I-PROD'] and li_i != len(target_str) - 1:
                    prod +=  _y
                    _prods_index.append(li_i)
                elif _y_ in ['B-PROD', 'I-PROD'] and li_i == len(target_str) - 1:
                    prod +=  _y
                    _prods_index.append(li_i)
                    prods.append(prod)
                    prod = ''
                    prods_index.append((min(_prods_index),))
                    _prods_index = []
                elif _y_ not in ['B-PROD', 'I-PROD'] and prod != '':
                    prods.append(prod)
                    prods_index.append((min(_prods_index), max(_prods_index)))
                    _prods_index = []
                    prod = ''
                if _y_ in ['B-QNT', 'I-QNT'] and li_i != len(target_str) - 1:
                    qnty +=  _y
                    _nums_index.append(li_i)
                elif _y_ in ['B-QNT', 'I-QNT'] and li_i == len(target_str) - 1:
                    qnty +=  _y
                    _nums_index.append(li_i)
                    qnties.append(qnty)
                    qnty = ''
                    nums_index.append((min(_nums_index),))
                    _nums_index =[]
                elif _y_ not in ['B-QNT', 'I-QNT'] and qnty != '':
                    qnties.append(qnty)
                    nums_index.append((min(_nums_index), max(_nums_index)))
                    _nums_index = []
                    qnty = ''
            prods_indexes.append(prods_index)
            nums_indexes.append(nums_index)
        return (prods_indexes, nums_indexes)


    def match(self, prods_index, nums_index, target_str):
        # prods_index, nums_index, target_str = get_prod_and_num_index(target, output)[0], get_prod_and_num_index(target, output)[1], target_str
        print('match 1', prods_index, nums_index)
        print('match 1.1', len(prods_index), len(nums_index))
        
        no_prod_and_has_one_num  = 0
        no_prod_and_has_multi_num = 0
        if not len(prods_index):
        # 先確認有沒有抓到prods
        # 如果沒有找到production, 但若有找到單個量詞的話, 可能只是user沒有設定產品名稱
        # 如: 【4盒 15x9.5x12】
            if len(nums_index) == 1:
                # !!! 這裡確實是nums_index_list, 而非nums_index,
                # 因為一個nums_index_list裡面可能有多組而非nums_index, 代表有複數個產品
                # 但我們現在要處理的狀況是: 找不到商品, 但有單個量詞, 所以我們傳回單個量詞
                no_prod_and_has_one_num = 1
            else:
                no_prod_and_has_multi_num = 1
                return ''
        # 如果prod先行, 代表num相鄰兩prods距離相等時, 當作左方prod的後綴, 如 『大漢堡 7』 小漢堡
        # 若num先行, 此時當作 大漢堡 『7 小漢堡』

        # prods_index
        # [(12, 18), (21, 22), (26, 29), (33,)]
        # nums_index
        # [(19, 19), (24, 24), (30, 31)]

        # 假定：
        #   1. 可能有prod沒有num註記, 但不會有num註記找不到對應的prod
        #       >> 因此對應不到的num就不管它惹
        #       >> 沒有num註記的prod, 我們補一個1給它
        #       >> 以each_num為中心, 計算其與相鄰prod的距離

        order_dict = {}

        for i, each_index in enumerate([prods_index, nums_index]):
            for ii, each_tuple in enumerate(each_index):
                key = (i, ii)
                order_dict[key] = (np.mean(each_tuple), each_tuple)

        order_dict = sorted(order_dict.items(), key=lambda x:x[1][0])

        for i, _ in enumerate(order_dict):
            # 重新排版
            order_dict[i] = [_[0], _[1][0], _[1][1]]
        # [key_tuple, mean_of_its_indexes, its_indexes]
        # key_tuple: (0, x) >> prod, (1, x) >> num
        print('match 2', order_dict)

        opt_prods = []
        opt_nums = []
        prod_first = 1 if order_dict[0][0][0] == 0 else 1
        print('match 2.1', prod_first)
        to_index = lambda target_li, the_tuple: target_li[the_tuple[0]:the_tuple[1]+1] if len(the_tuple)==2 else target_li[the_tuple[0]:]

        if len(order_dict) == 1:
            # 只有一個元素
            if prod_first == 1:
                # 該元素為產品
                # opt_nums.append(to_index(target_str,order_dict[0][-1]))
                opt_prods.append(to_index(target_str,order_dict[0][-1]))
                opt_nums.append(1)
            else:
                if no_prod_and_has_one_num == 1:
                    # 只有一個量詞元素, 沒有產品元素
                    pass

        if len(order_dict) > 1 or no_prod_and_has_one_num == 1:
            # 有多個元素
            if prod_first == 1:
                # 且產品名稱先行
                print('match 3', 'multi-prods and prod first')
                for i, each_element in enumerate(order_dict):
                    if each_element[0][0] == 0:
                        # 找到產品了
                        opt_prods.append(to_index(target_str,each_element[-1]))
                        try:
                            if order_dict[i+1][0][0] == 1:
                                # 下一位為數量
                                opt_nums.append(to_index(target_str,order_dict[i+1][-1]))
                            else:
                                # 下一位又是產品名稱
                                opt_nums.append(1)
                        except:
                            # 沒有下一位了
                            opt_nums.append(1)
            else:
                # 數量先行
                print('match 3', 'num go first.')
                for i, each_element in enumerate(order_dict):
                    if no_prod_and_has_one_num == 1:
                        print('match 3.1', 'no prod and only have one number.')
                        # 沒有產品, 只有一個量詞
                        opt_nums.append(to_index(target_str,each_element[-1]))
                        opt_prods.append('None')
                        break

                    if each_element[0][0] == 0:
                        # 找到產品了
                        opt_prods.append(to_index(target_str,each_element[-1]))
                        try:
                            if order_dict[i-1][0][0] == 1:
                                # 上一位為數量
                                opt_nums.append(to_index(target_str,order_dict[i-1][-1]))
                            else:
                                # 上一位又是產品名稱, 補1
                                opt_nums.append(1)
                        except:
                            # 沒有上一位了, 不可能發生, 所以不寫
                            continue
        return (opt_prods, opt_nums)


    def match_alicia(self, prods_index_list, nums_index_list, target_str_list):
        # 相關註記請參照 match()
        # 這個函式只是 match() 的輸入跟輸出改成list而已
        opt_prods_list, opt_nums_list = [], []
        for prods_index, nums_index, target_str in zip(prods_index_list, nums_index_list, target_str_list):
            print('match_alicia 1:', prods_index, nums_index, target_str)
            print('match_alicia 1.1:', prods_index=='', nums_index=='', target_str=='')
            print('match_alicia 1.2:', prods_index is None, nums_index is None, target_str is None)
            print('match_alicia 1.3:', pd.isnull(prods_index), pd.isnull(nums_index), pd.isnull(target_str))
            print('match_alicia 1.4:', len(prods_index), len(nums_index), len(target_str))
            if not len(prods_index) + len(nums_index) == 0:
                # 代表【商品名稱】或【量詞】至少有找到一種
                opt_prods, opt_nums = self.match(prods_index, nums_index, target_str)
                opt_prods_list.append(opt_prods)
                opt_nums_list.append(opt_nums)
            else:
                # 代表【商品名稱】或【量詞】一種都沒看到，此時我們應該呈現出該字串完整的樣子，避免user搞混
                opt_prods_list.append([target_str,])
                opt_nums_list.append(['1',])
                
        return (opt_prods_list, opt_nums_list)


    def get_annotations_alicia(self, target_str_list):
        _input_list = self.preclean_seq(target_str_list)

        with self.graph.as_default():
            _output_list = self.model.predict(_input_list)

        prods_index_list, nums_index_list = self.get_prod_and_num_index_alicia(target_str_list, _output_list)
        print('get_annotations_alicia 1', prods_index_list, nums_index_list)
        try:
            opt_prods, opt_nums =  self.match_alicia(prods_index_list, nums_index_list, target_str_list)
            print('get_annotations_alicia 2', opt_prods, opt_nums)
        except:
            opt_prods, opt_nums = 'None', 'None'
            print('get_annotations_alicia 2: something went wrong!!')

        return opt_prods, opt_nums


    def get_results_alicia(self, contents_in_array, nums_in_array):
        # kashgari model資料處理流程如下:
        # 將content_in_array送到first_clean()函式做處理, 處理後回傳:
        #       is_gift : 是否為禮盒組 0|1
        #       clean_content : 清洗後的content
        #       multiple : 組數, 類似右側範例中的"int(2)" >> 【金牌大師 滴雞精】5盒 * 2組
        #       _prfix : 前綴詞, 像是預購、禮盒、出貨/到貨日期等
        # 與此同時一起處理nums, 將nums_in_array處理成[int, int, int...]的型態
        # > > > > > > > > > > > > > > > > > > > > > > > > > > > > > > > >
        # 接著將clean_content(array)餵進get_annotations_alicia()
        # 而後會產出opt_prods, opt_nums兩組陣列
        # 再將「opt_prods, opt_nums, nums_in_array, is_gift, multiple, _prefix」
        # 這6組資訊餵進second_clean()函式, 就能產出樂天派需要的規格(sub_content)了!!

        # 但為了加快速度, 我將先將餵進來的contents_arr資料變成字典形式
        # d = {0: c1, 1: c2..}
        # 再取set(c1)的部分丟進kashgari model
        # 最後再以d將parsing後資訊映射回來

        content_by_index_dict = {}
        for _, c in enumerate(contents_in_array):
            content_by_index_dict[_] = c
        # 這個字典用來儲存每一個index對應的content
        unique_contents = np.array([_ for _ in set(contents_in_array)])
        is_gift_arr, clean_content_arr, multiple_arr, _prefix_arr = [], [], [], []
        
        for _ in unique_contents:
            is_gift, clean_content, multiple, _prefix = first_clean(_)

            is_gift_arr.append(is_gift)
            clean_content_arr.append(clean_content)
            multiple_arr.append(multiple)
            _prefix_arr.append(_prefix)


        clean_num = lambda x: 1 if len(x)==0 else int(x)
        nums_in_array = [clean_num(str(_)) for _ in nums_in_array]

        opt_prods, opt_nums = self.get_annotations_alicia(clean_content_arr)
        print('get_results_alicia 1', opt_prods, opt_nums)
        # opt_prods, opt_nums = kash.get_annotations_alicia(clean_content_arr)
        # 這裡都還是unique contents from contents_in_array

        temp_final_outputs = []

        for each_key in content_by_index_dict:
            #break
            mapped_index = int(np.where(unique_contents==content_by_index_dict[each_key])[0])
            # 列出該元素在unique_contents的位置
            
            mapped_opt_num = opt_nums[mapped_index]
            mapped_num = nums_in_array[each_key]
            mapped_is_gift = is_gift_arr[mapped_index]
            mapped_multiple = multiple_arr[mapped_index]
            
            mapped_opt_prod = opt_prods[mapped_index]
            mapped_prefix = _prefix_arr[mapped_index]
            print('get_results_alicia 1: ', mapped_opt_prod, mapped_opt_num, 
                mapped_num, mapped_is_gift, mapped_multiple, mapped_prefix)
            temp_final_outputs.append(second_clean(mapped_opt_prod, mapped_opt_num,
                                                   mapped_num, mapped_is_gift,
                                                   mapped_multiple, mapped_prefix))

        return temp_final_outputs



if __name__ == '__main__':
    kash = kashgari_model()
    target = '彰化元老級名店水根肉乾/圓燒檸檬豬肉乾1 豬肉*7 檸檬肉乾52 檸檬肉紙'
    #target = '4盒 15x9.5x12'
    
    print(kash.get_annotations(target))

    #df = pd.read_csv('test_folder/orders (19).csv')
    #a = df['Product Name'].apply(lambda x: x.split('】')[-1].split('-')[-1].strip() if '-' in x else x.split('】')[-1].strip())
    #b = df['Quantity Ordered'].astype(str).tolist()
    #final_outputs = kash.get_results_alicia(a, b)
    #with open('temp_result.txt', 'w') as w:
    #    for _ in final_outputs:
    #            w.write(_ + '\n')

    sleep(2)
    print('READY')

    while(1):
        the_input = input()
        #with open('kash_with_alicia_log.txt', 'a') as w:
            #w.write('THE INPUT\n')
            #w.write(the_input)
            #w.write('\n\n')
        try:
            the_input = eval(the_input)
            # 如果輸入的是list型態, 則第一個元素為產品名稱list, 第二個元素為數量list
        except:
            continue        

        if type(the_input) is list:
            # 代表輸入的其實是list >> 啟用list模式的kash_model
            #with open('kash_with_alicia_log.txt', 'a') as w:
                #w.write('List Detail\n')
                #w.write(the_input[0] + '  ,  ' + the_input[1] + '\n')
                #w.write('\n')
            final_outputs = kash.get_results_alicia(the_input[0], the_input[1])
            print('done_parsing')
            print(final_outputs)

# [["蔣老爹牛霸王牛肉麵麵組_牛肉麵*3+牛渣渣乾麵*2",], ["1",]]
# [["蔣老爹牛霸王牛肉麵麵組_牛肉麵*3+牛渣渣乾麵*2(牛肉麵600g+牛渣渣乾麵/270g)",], ["1",]]
# [["牛肉麵*3+牛渣渣乾麵*2",], ["1",]]
# [["牛肉麵*3+牛渣渣乾麵*2(牛肉麵600g+牛渣渣乾麵/270g)",], ["1",]]
# [["牛肉麵*3+牛渣渣乾麵*2(牛肉麵600g+牛渣渣乾麵/270g)牛肉麵*3+牛渣渣乾麵*2",], ["1",]]
# [["牛肉麵*3+牛渣渣乾麵*2", "特殊組合可於備註欄填寫"], ["2", "1"]]