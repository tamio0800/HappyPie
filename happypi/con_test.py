con = sqlite3.connect('db.sqlite3')
wb = pd.read_excel(r'D:\Users\edony\Desktop\happy\happypi\con_test.xlsx')

wb.to_sql(name = 'test', con = con, if_exists = 'append' )


con.execute("SELECT * FROM History_data").fetchall()