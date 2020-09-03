# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 08:34:11 2020

@author: common tata
"""

import re
import numpy as np


# 預購、幾日出貨等一起做
# 【禮盒*3 : 2號*3 + 3號*3】  變成這樣  3罐禮盒組:2號*9 + 3號*9

def n_plus_m_clean(target):
    # 清理15+1之類的格式
    n_plus_m_pattern = re.compile(r'\d+[ ]{0,1}[+加][ ]{0,1}\d+|[( ][\w]{0,4}\d+[\w]{0,4}[送贈][\w]{0,4}\d+[)]{0,1}')
    # 2020.06.03 新增 滿xx送/加贈xx...格式

    if len(re.findall(n_plus_m_pattern, target)):
        # print(re.findall(n_plus_m_pattern, target))
        # 找到類似的型態
        for each_pair in re.findall(n_plus_m_pattern, target):
            num_0, num_1 = re.findall(r'\d+', each_pair)
            num_sum = str(to_num(num_0) + to_num(num_1))
            # 加總後, 開始進行取代原先的文字
            target = target.replace(each_pair, '*' + num_sum)
    return target


def han_num_and_units_pre_clean_trans(_opt_nums):
    # 本function用在將han_nums與units互相結合的狀況轉成阿拉伯數字+units
    units = '串件份伙個入副包匹只堆塊對尾打批把捆支朵杯枚條片瓶盒碗窩箱粒組罐群袋隻雙項顆斤套'
    # han_nums = '一二三四五六七八九十壹貳參肆伍陸柒捌玖拾兩倆單全'

    han_num_and_units_pattern = re.compile(r'[一二三四五六七八九十壹貳參肆伍陸柒捌玖拾兩倆單]{1,5}[ ]{0,1}[串件份伙個入副包匹只堆塊對尾打批把捆支朵杯枚條片瓶盒碗窩箱粒組罐群袋隻雙項顆斤套]')
    han_num_pattern = re.compile(r'[一二三四五六七八九十壹貳參肆伍陸柒捌玖拾兩倆單全]{1,5}')

    # !!! 若只有units?
    _temp_opt = []
    for each_num in _opt_nums:
        each_num = str(each_num)
        if len(re.findall(han_num_and_units_pattern, each_num)):
            # 該元素符合類似 一組 之類的字眼
            han_num_part = re.findall(han_num_pattern, each_num)[0]
            _temp_opt.append(str(num_trans(han_num_part)) + each_num[-1])
            # 將轉換後的han_num與units重新組合後回傳
        elif len(each_num) == 1 and each_num in [_ for _ in units]:
            # 若只有units, 則在前面加上 "1" 後返回 >> 組 -> 1組
            _temp_opt.append('1' + each_num)
        else:
            _temp_opt.append(each_num)
        print('hn & upct 1:',each_num, _temp_opt[-1])
    return _temp_opt

def get_first_matched_pattern(re_pattern, target):
    if len(re.findall(re_pattern, target)) == 0:
        return ''
    else:
        return re.findall(re_pattern, target)[0]

def first_clean(target):
    # return (is_gift-bool, clean_target-str, multiple-int)
    # target = '美安獨家【金牌大師 - 滴雞精】3盒(每盒/10包)*2'
    target = target.strip().replace('-網', '')   
    _prefix, target = check_pre_order(target)
    
    # 新增 大包/小包/獨享 等前綴詞
    size_pattern = re.compile(r'大份量|小份量|大包裝|小包裝|獨享|大份量|小包|大包')
    size_pattern_matched_string = get_first_matched_pattern(size_pattern, target)
    if len(size_pattern_matched_string) > 0:
        _prefix = '(' + get_first_matched_pattern(size_pattern, target) + ') ' + _prefix
    
    print('fc1', 'check pre order successfully.')
    print('fc1.1', _prefix, target)

    jianglaodie_pattern = re.compile(r'蔣老爹牛霸王牛肉麵麵組[_ -]{0,1}|蔣老爹牛霸王牛肉麵組[_ -]{0,1}')
    target = re.sub(jianglaodie_pattern, '', target)
    target = re.sub(re.compile(r'\('), ' (', target)
    print('fc1.2', target)

    pattern_could_be_multi_gift = \
    re.compile(r'[(（ _-]\w{0,2}禮盒組\w{0,2}[)） _-]{0,1}|[ -_]禮盒組[ ]{0,1}[*xX][ ]{0,1}\d+$|\w{0,2}禮盒[組]{0,1}[\d]{0,2}[*xX]\d+$|^禮盒[組]{0,1}[\d]{0,2}[*xX]\d+[ ]{0,1}[:：]|[)） _-]禮盒[組]{0,1}[ ]{0,1}[*xX]{0,1}[\d]{0,2}$|\d+\w禮盒[組]{0,1}[ ]{0,1}[*xX][ ]{0,1}\d+')
    pattern_not_multi_gift = \
    re.compile(r'^[\w]{0,3}禮盒[組]{0,1}[\d]{0,1}[ ]{0,1}[-:： _]|\d+\w禮盒[組]{0,1}')
    # 2020.06.04 新增  "【預購_青葉台菜】憶起呷嬤粽10顆禮盒組(180g± 10g/顆:5顆/盒)5/30-6/6" 此類的禮盒組規則

    exclude_pattern = re.compile(r'\d+\w禮盒')
    # 2020.06.04 新增 不希望誤拿重要資訊
    pattern_ji_jing = re.compile(r'【.+滴雞精】[ ]{0,1}\d+盒')

    pattern_multiple_at_tail = re.compile(r'[^*\n\t\r]+\D[ ]{0,1}[Xx*][ ]{0,1}\d+[組]{0,1}$')
    # 2020.06.07 新增 在這裡加上辨識有沒有類似PPQQ * int 或 PPQQ, PPQQ, PPQQ * int之類的格式
    # 前面整段必須沒有[Xx*]等連結符號，且該int必須位於最末處，且倒數第三類符號不為數字

    # 2020.06.08 新增 遇到 "蔣老爹霸王蝦水餃(20顆/包)(任選888)*2" 時不知道該怎麼處理
    # 此時multiple預估為2, 但kash又偵測數量為2, 最終變成4, 暫時先停止這個功能
    #if len(re.findall(pattern_multiple_at_tail, target)):
    #    print('fc2', 'found multiple!', re.findall(pattern_multiple_at_tail, target)[0])
    #    multiple = int(re.findall(re.compile(r'\d+[組]{0,1}$'),
    #                   re.findall(pattern_multiple_at_tail, target)[0])[0].replace('組', ''))
    #else:
    #    print('fc2', 'not found multiple!')
    #    multiple = 1

    multiple = 1

    get_multiple = lambda certain_target: int(re.findall(r'\d+', re.findall(r'[ ]{0,1}[*xX][ ]{0,1}\d+', certain_target)[0])[0])
    is_gift = 0

    # 先檢查滴雞精
    if len(re.findall(pattern_ji_jing, target)):
        print('fc3', 'move into ji jing fraction.')
        # 發現奇怪的滴雞精標示, 接下來確認是否有組數
        clean_target = re.findall(pattern_ji_jing, target)[0].replace('】', '】 滴雞精 ').strip()
        if len(re.findall(r'[*xX]\d+組$|[)]{0,1}[*xX]\d+$', target)):
            # 有包含組數
            multiple *= int(re.findall(r'\d+', re.findall(r'[*xX]\d+組$|[)]{0,1}[*xX]\d+$', target)[0])[0])
            return(is_gift, clean_target, multiple, _prefix)

    if len(re.findall(pattern_could_be_multi_gift, target)):
        print('fc3', 'move into gift box with multiple fraction.')
        # 是有組數的禮盒組
        is_gift = 1
        certain_target = re.findall(pattern_could_be_multi_gift, target)[0]

        if len(re.findall(exclude_pattern, certain_target)):
            # certain_target 包含商品重要資訊
            certain_target = re.findall(re.compile(r'禮盒.+'), certain_target)[0]

        try:
            multiple *= get_multiple(certain_target)
        except:
            pass
        clean_target = target.replace(certain_target, ' ').strip()

        return(is_gift, clean_target, multiple, _prefix)

    elif len(re.findall(pattern_not_multi_gift, target)):
        print('fc3', 'move into gift box without multiple fraction.')
        # 是沒有組數的禮盒組
        is_gift = 1

        certain_target = re.findall(pattern_not_multi_gift, target)[0]

        # 2020.06.24註解掉, 因為不知道當初為甚麼會寫這段, 而且它會誤判：
        # 3罐禮盒組:1號+2號+3號  >>  【禮盒組】 1號*1, 2號*1, 3*1罐, 3號*1
        #if len(re.findall(exclude_pattern, certain_target)):
            # certain_target 包含商品重要資訊
        #    certain_target = re.findall(re.compile(r'禮盒.+'), certain_target)[0]

        clean_target = target.replace(certain_target, ' ').strip()
        return(is_gift, clean_target, multiple, _prefix)


    return (is_gift, target, multiple, _prefix)


def check_pre_order(target):
    pre_order_pattern = re.compile(r'[\d]{0,4}[/.-]{0,1}[01]{0,1}[0-9][/.-][0-3]{0,1}[0-9][ ]{0,1}[-_ ~到至][ ]{0,1}[\d]{0,4}[/.-]{0,1}[01]{0,1}[0-9][/.-][0-3]{0,1}[0-9]')

    pre_order_tag = ''
    has_receiving_day = 0
    has_shipping_day = 0

    prefix = ''

    for search_word in ['預購', '預定']:
        if search_word in target:
            pre_order_tag = '(預購) '
            target = target.replace(search_word, ' ')

    for search_word in ['到貨', '寄達', '送達']:
        if search_word in target:
            has_receiving_day = 1
            target = target.replace(search_word, ' ')

    for search_word in ['出貨', '寄貨', '寄送']:
        if search_word in target:
            has_shipping_day = 1
            target = target.replace(search_word, ' ')

    finding = re.findall(pre_order_pattern, target)
    prefix = pre_order_tag

    # 確認有預購

    if len(finding):
        target = re.sub(pre_order_pattern, ' ', target).strip()
        if has_receiving_day and len(finding):
            prefix = pre_order_tag + finding[0] + '到貨 '
        elif has_shipping_day and len(finding):
            prefix = pre_order_tag + finding[0] + '出貨 '
        else:
            prefix = pre_order_tag + finding[0] + ' '

    return prefix, target

def num_trans(han_num):

    _han_num = ''
    han_num_cap_dict = {'壹':'一', '貳':'二', '參':'三', '肆':'四', '伍':'五',
                        '陸':'六', '柒':'七', '捌':'八', '玖':'九', '拾':'十'}
    for each_han_num_cha in han_num:
        if each_han_num_cha in han_num_cap_dict.keys():
            _han_num += han_num_cap_dict[each_han_num_cha]
        else:
            _han_num += each_han_num_cha
    han_num = _han_num

    num_dict = {
        '一':1, '二':2, '三':3, '四':4, '五':5,
        '六':6, '七':7, '八':8, '九':9, '十':10,
        '兩':2, '倆':2, '單':1, '全':1}

    remove_han = lambda x: ''.join([_ for _ in x if _ in num_dict.keys()])
    chain_han = lambda x: int(''.join([str(num_dict[_]) for _ in x]))

    # han_num = '十八'
    han_num = remove_han(han_num)

    if han_num in num_dict.keys():
        return num_dict[han_num]
    else:
        if len(han_num) != 2:
            if han_num[-1] == '十':
                # X 10
                han_num = ''.join([_ for _ in han_num[:-1] if _ != '十'])
                return(chain_han(han_num) * 10)
            else:
                han_num = ''.join([_ for _ in han_num if _ != '十'])
                return(chain_han(han_num))
        else:
            # 兩位數
            if han_num[-1] == '十':
                # X 10
                han_num = ''.join([_ for _ in han_num[:-1] if _ != '十'])
                return(chain_han(han_num) * 10)
            else:
                if han_num[0] == '十':
                    han_num = '一' + han_num[-1]
                    return chain_han(han_num)
                else:
                    return chain_han(han_num)

def to_num(target):
    try:
        return int(target)
    except:
        return num_trans(target)

def second_clean(opt_prods, opt_nums, input_num, is_gift, multiple, prefix):
    # 先想辦法分離出單位量詞, 並記錄下來
    print('sc0', opt_prods, opt_nums)

    opt_nums = han_num_and_units_pre_clean_trans(opt_nums)
    opt_nums = [str(_) for _ in opt_nums]
    has_num, has_unit = 0, 0
    _temp_opt_nums = []
    _opt_nums_units = []
    num_pattern = re.compile(r'\d+')
    unit_pattern = re.compile(r'\D+')
    print('sc1', opt_nums)
    for _ in opt_nums:
        if len(re.findall(unit_pattern, _)):
            print('sc2', re.findall(unit_pattern, _))
            has_unit = 1
            # 找到不屬於數字的部分
            _opt_nums_units.append(re.findall(unit_pattern, _)[0])

            # 記錄量詞
        else:
            _opt_nums_units.append('')

        if len(re.findall(num_pattern, _)):
            _temp_opt_nums.append(re.findall(num_pattern, _)[0])
            has_num = 1

    if has_unit == 1 and has_num == 0:
        assert len(_opt_nums_units) == 1
        _temp_opt_nums.append('1')

    assert len(_temp_opt_nums) == len(_opt_nums_units)


    print('sc3', _temp_opt_nums, _opt_nums_units, has_unit)

    _opt_nums = []
    for _ in _temp_opt_nums:
        _opt_nums.append(to_num(_)*to_num(multiple)*input_num)


    _opt_nums = np.array(_opt_nums)
    print('sc4', _opt_nums)
    _opt_nums_units = np.array(_opt_nums_units)
    opt_prods = np.array([_.strip() for _ in opt_prods])

    final_prods, final_nums, final_units = [], [], []

    print('sc4.1', opt_prods, len(opt_prods))
    if not len(opt_prods) == 0:
        for each_prod in sorted(set(opt_prods)):
            final_prods.append(each_prod)
            final_nums.append(_opt_nums[opt_prods==each_prod].sum())
            final_units.append(sorted(_opt_nums_units[opt_prods==each_prod])[-1])
    else:
        print('go exception')
        final_prods.append('')
        final_nums.append(_opt_nums.sum())
        final_units.append(sorted(_opt_nums_units)[-1])

        # sorted(['', '', '5', '盒', '']) >> ['', '', '', '5', '盒']
    print('sc5', final_prods, final_nums, final_units)
    # print('final_prods', final_prods)
    # print('final_nums', final_nums)

    final_content = ''
    for i, j, k in zip(final_prods, final_nums, final_units):
        # print(i, j)
        # print(str(i) + '*' + str(j) + ', ')
        if not i == '':
            final_content += i + '*' + str(j) + k + ', '
        else:
            final_content += str(j) + k + ', '

    final_content = final_content[:-2]

    if is_gift:
        return prefix + '【禮盒組】 ' + final_content
    else:
        return prefix + final_content



if __name__ == '__main__':

    #print(first_clean('【預購_青葉台菜】憶起呷嬤粽10顆禮盒組(180g± 10g/顆:5顆/盒)5/30-6/6'))

    string ='【預購_青葉台菜】憶起呷嬤粽10顆禮盒組(180g± 10g/顆:5顆/盒)5/30-6/6'
    #tar = [string for _ in range(20000)]

    from time import time

    st = time()
    for i in range(20000):
        first_clean(string)
    ed = time()
    print(ed - st)


    tar = [string for _ in range(10)]
    st = time()
    np.apply_along_axis(first_clean, 0, tar)
    ed = time()
    print(ed - st)




