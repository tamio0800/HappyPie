import pandas as pd

df = pd.read_excel('20201104-233142_待處理訂單資料整合檔.xlsx')
vendors = list(set([_[1:-1] for _ in df['內容物'].str.extract(r'(【.*】)').iloc[:, 0].tolist() if not pd.isnull(_)]))