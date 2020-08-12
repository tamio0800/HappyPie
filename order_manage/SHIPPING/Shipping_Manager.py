from datetime import datetime, timedelta
import pyDes
import base64

def generate_shipping_url(shipping_number, logistic_company):
    # logistic_company 有兩個選項, "black_cat" 或是 "xinzhu"
    def to_int_in_string_format(target):
        try:
            return str(int(target))
        except:
            return str(target)
    
    if logistic_company == 'xinzhu':
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
    elif logistic_company == 'black_cat':
        return 'https://www.t-cat.com.tw/Inquire/TraceDetail.aspx?BillID=' + shipping_number