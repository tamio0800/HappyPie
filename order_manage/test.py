from django.test import TestCase
from order_manage.models import History_data


class system_upgrade_201121_test(TestCase):

    def test_if_this_could_find_the_right_page(self):
        response = self.client.get('/order_manage/ordertracking/')  
        # 前後都要加【/】 !!!!!
        self.assertIn('訂單整合', response.content.decode())
        #print(response.items())
        self.assertTemplateUsed(response, 'order_manage/ordertracking.html')
        # print(response.content.decode())
        

    def test_if_excel_file_uploaded_correctly(self):
        file_path = 'test_folder/20201119_export_default (1).xls'
        uploaded_file = open(file_path, 'rb')
        self.client.post('/order_manage/ordertracking.html', {'files': uploaded_file})


    
        
        




