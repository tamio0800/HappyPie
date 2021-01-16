import pandas as pd
import re


def aggregate_elements_in_subcontent(target_string):
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

df_new = pd.read_excel('/mnt/c/Users/User/Desktop/HP_PROJECT/this_time_development/AB對比/整理後確認ok的官網版本.xlsx')
df_old = pd.read_excel('/mnt/c/Users/User/Desktop/HP_PROJECT/this_time_development/AB對比/舊版.xlsx')

df_old['規格'] = df_old['規格'].apply(lambda x: re.sub(r'[+]\s{0,3}紅包\s{0,3}\d+', '', x))
print(df_old['規格'].tolist()[:50]) 


'''
比對的參數：
    訂單編號-金額-規格
    有任一不同即列出

產出格式：
   | 是否符合 | 錯誤型態 | 訂購人 | 訂單編號(舊) | 訂單編號(新) | 金額(舊) | 金額(新) | 規格(舊) | 規格(新)

'''

result = pd.DataFrame(columns=['是否符合', '錯誤型態', '訂購人', '訂單編號(舊)', '訂單編號(新)', '金額(舊)',
     '金額(新)', '規格(舊)', '規格(新)'])

# 先找出舊的有的，新的沒有的
old_t_new_f_df = df_old[~df_old['訂單編號'].isin(df_new['訂單編號'])]
for each_txn_id in old_t_new_f_df['訂單編號'].unique():
    tdf = old_t_new_f_df[old_t_new_f_df['訂單編號']==each_txn_id].reset_index(drop=True)
    for each_index in tdf.index:
        result.loc[result.shape[0]] = \
            [
                'X',
                '舊的有，新的沒有',
                tdf.loc[each_index, '訂購人'],
                tdf.loc[each_index, '訂單編號'],
                '',
                tdf.loc[each_index, '金額'],
                '',
                aggregate_elements_in_subcontent(tdf.loc[each_index, '規格']),
                ''
            ]
print(f'first {result}')

# 再找出新的有，舊的沒有的
old_f_new_t_df = df_new[~df_new['訂單編號'].isin(df_old['訂單編號'])]
for each_txn_id in old_f_new_t_df['訂單編號'].unique():
    tdf = old_f_new_t_df[old_f_new_t_df['訂單編號']==each_txn_id].reset_index(drop=True)
    for each_index in tdf.index:
        result.loc[result.shape[0]] = \
            [
                'X',
                '新的有，舊的沒有',
                tdf.loc[each_index, '訂購人'],
                '',
                tdf.loc[each_index, '訂單編號'],
                '',
                tdf.loc[each_index, '金額'],
                '',
                aggregate_elements_in_subcontent(tdf.loc[each_index, '規格'])
            ]
print(f'second {result}')

# 最後是兩者都有，檢查一不一致
tdf_new = df_new[df_new['訂單編號'].isin(df_old['訂單編號'])]
tdf_old = df_old[df_old['訂單編號'].isin(df_new['訂單編號'])]

for each_txn_id in tdf_new['訂單編號'].unique():
    _tdf_new = tdf_new[tdf_new['訂單編號']==each_txn_id].reset_index(drop=True)
    _tdf_old = tdf_old[tdf_old['訂單編號']==each_txn_id].reset_index(drop=True)

    _tdf_new.loc[:, 'unique_id'] = _tdf_new['訂單編號'].astype(str) + _tdf_new['供應商']
    _tdf_old.loc[:, 'unique_id'] = _tdf_old['訂單編號'].astype(str) + _tdf_old['供應商']

    if _tdf_new.shape[0] == 1 and _tdf_old.shape[0] == 1:
        # 都只有一個，可以直接比對
        try:
            if not ((_tdf_old.loc[0, '訂購人'] == _tdf_new.loc[0, '訂購人']) and 
                (_tdf_old.loc[0, '金額'] == _tdf_new.loc[0, '金額']) and 
                (aggregate_elements_in_subcontent(_tdf_old.loc[0, '規格']) == aggregate_elements_in_subcontent(_tdf_new.loc[0, '規格']))):
                result.loc[result.shape[0]] = \
                [
                    'X',
                    '內容不一致',
                    _tdf_old.loc[0, '訂購人'],
                    _tdf_old.loc[0, '訂單編號'],
                    _tdf_new.loc[0, '訂單編號'],
                    _tdf_old.loc[0, '金額'],
                    _tdf_new.loc[0, '金額'],
                    aggregate_elements_in_subcontent(_tdf_old.loc[0, '規格']),
                    aggregate_elements_in_subcontent(_tdf_new.loc[0, '規格'])
                ]
            else:
                # 一致
                result.loc[result.shape[0]] = \
                [
                    'O',
                    '一致',
                    _tdf_old.loc[0, '訂購人'],
                    _tdf_old.loc[0, '訂單編號'],
                    _tdf_new.loc[0, '訂單編號'],
                    _tdf_old.loc[0, '金額'],
                    _tdf_new.loc[0, '金額'],
                    aggregate_elements_in_subcontent(_tdf_old.loc[0, '規格']),
                    aggregate_elements_in_subcontent(_tdf_new.loc[0, '規格'])
                ]
        except Exception as e:
            print(f'ERROR_1: {e}')
    else:
        # 數量不一致
        print(f'Muliti-orders')
        try:
            for each_unique_id in _tdf_new.unique_id.unique():
                _ttdf_new = _tdf_new[_tdf_new.unique_id==each_unique_id]
                _ttdf_old = _tdf_old[_tdf_old.unique_id==each_unique_id]

                if not ((_ttdf_new['訂購人'].tolist()[0] == _ttdf_old['訂購人'].tolist()[0]) and 
                    (_ttdf_new['金額'].tolist()[0] == _ttdf_old['金額'].tolist()[0]) and 
                    (aggregate_elements_in_subcontent(_ttdf_new['規格'].tolist()[0]) == aggregate_elements_in_subcontent(_ttdf_old['規格'].tolist()[0]))):
                    result.loc[result.shape[0]] = \
                    [
                        'X',
                        '內容不一致',
                        _ttdf_old['訂購人'].tolist()[0],
                        _ttdf_old['訂單編號'].tolist()[0],
                        _ttdf_new['訂單編號'].tolist()[0],
                        _ttdf_old['金額'].tolist()[0],
                        _ttdf_new['金額'].tolist()[0],
                        aggregate_elements_in_subcontent(_ttdf_old['規格'].tolist()[0]),
                        aggregate_elements_in_subcontent(_ttdf_new['規格'].tolist()[0])
                    ]
                else:
                # 一致
                    result.loc[result.shape[0]] = \
                    [
                        'O',
                        '一致',
                        _ttdf_old['訂購人'].tolist()[0],
                        _ttdf_old['訂單編號'].tolist()[0],
                        _ttdf_new['訂單編號'].tolist()[0],
                        _ttdf_old['金額'].tolist()[0],
                        _ttdf_new['金額'].tolist()[0],
                        aggregate_elements_in_subcontent(_ttdf_old['規格'].tolist()[0]),
                        aggregate_elements_in_subcontent(_ttdf_new['規格'].tolist()[0])
                    ]
        except Exception as e:
            print(f'ERROR_2: {e}')
            print(f'ERROR_2 INFO {each_unique_id}')
            print(f'ERROR_2 INFO NEW {_tdf_new[_tdf_new.unique_id==each_unique_id]}')
            print(f'ERROR_2 INFO OLD {_tdf_old[_tdf_old.unique_id==each_unique_id]}')

print(f'third {result}')
result.to_excel('/mnt/c/Users/User/Desktop/HP_PROJECT/this_time_development/AB對比/比對後結果.xlsx', index=False)
