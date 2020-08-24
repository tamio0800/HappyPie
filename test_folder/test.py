import pandas as pd

def _combine_columns(combine_1_dim_array, linked, only_meaningful=False):
    _temp = ''
    if not only_meaningful:
        for _, _element in enumerate(combine_1_dim_array):
            _element = _element.strip()
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
            _element = _element.strip()
            if not (pd.isnull(_element) or _element == '' or _element == '共同'):
                if _ == 0:
                    _temp += str(_element)
                else:
                    _temp += linked + str(_element)
        if len(_temp) == 0:
            # 沒看到什麼重要的資訊
            _temp = combine_1_dim_array[0].strip()
    return _temp



x = _combine_columns(['【韓國不倒翁OTTOGI】起司拉麵+泡菜風味拉麵(2+1組合)-網', '共同', '共同'], ', ', True)
print(x)