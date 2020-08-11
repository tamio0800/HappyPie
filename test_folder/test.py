import pandas as pd
import re


_temp = {'好吃市集': re.compile(r'^2[0-9]{3}-[0-9]{2}-[0-9]{2}_好吃市集_\S+'),
        '生活市集': re.compile(r'^2[0-9]{3}-[0-9]{2}-[0-9]{2}_生活市集_\S+'),
        '樂天派官網': re.compile(r'.*export_[0-9]{2}\w{3}[0-9]{2}\s{0,2}.*xls[x]{0,1}$'),
        'MOMO': re.compile(r'[A-Z]\d+_\d_\d+_\d+_[20]\d+.xls|\S+\d+\s{0,2}[(]MOMO[)].xls|.*訂單查詢-第三方物流.*xls[x]{0,1}$|[A-Z]\d+_\d_\d+_[20]\d+.xls'),
        '亞伯': re.compile(r'[a-z]\d+_PoDetail_\d+.xls|\S+PoDetail_\d+\s{0,2}[(]亞伯[)].xls|[a-z]\d+_shipmentReport_\d+.xls'),
        '東森得易購': re.compile(r'^[a-z0-9]{8}_20\d+.xls'),
        'Yahoo購物中心': re.compile(r'^delivery - [0-9]{4}-[0-9]{2}-[0-9]{2}\S+\s{0,2}[(]YAHOO購物中心[)].xls|^delivery - [0-9]{4}-[0-9]{2}-[0-9]{2}\S+\s{0,2}.xls'),
        'UDN': re.compile(r'^Order_2[0-9]{16}[(][Uu][Dd][Nn][)]'),
        'Friday': re.compile(r'^OrderData_[0-9]{5} - 2[0-9]{3}-[0-9]{2}-\S+.csv'),
        '博客來': re.compile(r'^take_order_2[0-9]{13}\s{0,2}[(]博客來[)].xls|^take_order_2[0-9]{13}\s{0,2}.xls'),
        '台塑': re.compile(r'^Order_2[0-9]{16}[(]台塑[)]'),
        '整合檔': re.compile(r'^20[0-9]{6}-[0-9]{6}_\S*整合檔\S*.xls[x]{0,1}'),
        'LaNew': re.compile(r'.*_\w{5}_2[0-9]{3}[01][0-9][0123][0-9].xls[x]{0,1}'),
        }

for name, p in _temp.items():
    if len(re.findall(p, '複本 訂單接單_XWQ00_20200804.xlsx')) > 0:
        print(name, re.findall(p, '複本 訂單接單_XWQ00_20200804.xlsx')[0])


#pattern = re.compile(r'.*_\w{5}_2[0-9]{3}[01][0-9][0123][0-9].xls[x]{0,1}')
#print(re.findall(pattern, '複本 訂單接單_XWQ00_20200804.xlsx'))

#df = pd.read_excel('複本 訂單接單_XWQ00_20200804.xlsx')
#print(df)

