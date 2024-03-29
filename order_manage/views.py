# -*- coding: utf8 -*- 
from django.shortcuts import render, redirect
from numpy.core.fromnumeric import sort
from order_manage.models import History_data
from django.views import View
from django.http import HttpResponse , FileResponse
from django.contrib.auth.decorators import login_required
from django.core.files.storage import FileSystemStorage
from order_manage.ALICIA import Alicia_0611  # 匯入ALICIA
import pandas as pd, numpy as np
import os
from time import time, sleep, localtime
import subprocess
from order_manage.model_tools import HISTORY_DATA_and_Subcontent_user_edit_record_db_writer
from django_pandas.io import read_frame
from order_manage.SHIPPING.Shipping_Manager import *

try:
    kash = subprocess.Popen(['/home/edony-prod/miniconda3/envs/happypie/bin/python', os.path.join(os.getcwd(),'order_manage','KASH','kashgari_final_with_Alicia.py')],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
except:
    kash = subprocess.Popen(['python', os.path.join(os.getcwd(),'order_manage','KASH','kashgari_final_with_Alicia.py')],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
print('loading model.')

is_ready = 0
while(is_ready == 0):
    try:
       opt = kash.stdout.readline().decode().strip()
       print(opt)
       if opt == 'READY':
           is_ready = 1   
    except:
        print('Wait for loading.')
        sleep(2.5)
        continue
print('Model has loaded.')

# 將利用kashgari來分析寫成一個函式方便使用
def kashgari_parsing(prod_ipt, num_ipt):

    alicia = Alicia_0611.ALICIA()
    alicia.wait_till_the_flag_comes_up(
        'all_flags/kashgari_model_is_not_running.flag',
        'all_flags/kashgari_model_is_running.flag')
    # 這裡其實應該用kashgari_model_is_not_running.flag來判別會比較好
    # 但這樣我就要回頭去修改order_tracking, 然而Annie跟薪芫也正在修改那份檔案
    # 所以等大家都完成後我再回頭優化吧!
    try:
        kash.stdin.write((str([prod_ipt, num_ipt]) + '\n').encode())
        kash.stdin.flush()
        wait_for_kashgari_model_done_parsing()
        result = eval(kash.stdout.readline().decode().strip())
    except:
        print('kashgari_parsing', 'return nothing.')
        result = []
        os.rename(
                  'all_flags/kashgari_model_is_running.flag',
                  'all_flags/kashgari_model_is_not_running.flag')
        return result

    os.rename(
              'all_flags/kashgari_model_is_running.flag',
              'all_flags/kashgari_model_is_not_running.flag')
    return result


def to_download_file(request):
    # print('start 1')
    download_from_which_folder = 'download_file'
    # print('start 2')
    def write_current_pending_txns_to_excel_file(which_folder_to_save='download_file'):
        alicia = Alicia_0611.ALICIA()
        # print('write_current_pending_txns_to_excel_file 1')
        model_writer = HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=alicia.aggregated_txns)
        # print('write_current_pending_txns_to_excel_file 2')
        _df = model_writer.query_all_pending_txns()
        # print('write_current_pending_txns_to_excel_file 3')
        _df = alicia._clean_dataframe(
            pandas_dataframe=_df,
            strip_only=True, 
            make_null_be_nullstring=True,
            easy_read_for_users=True, 
            dealing_columns=['貨到付款', '回押', '已寄出', '已取消'])
        # print(_df.shape)
        # print(_df.head())
        # print('write_current_pending_txns_to_excel_file 4')
        _df_name = alicia.get_today("%Y%m%d-%H%M%S") + '_待處理訂單資料整合檔.xlsx'
        # _df['規格'] = _df['規格'].apply(lambda x: alicia.aggregate_elements_in_subcontent(x))
        # print(alicia.get_today("%Y%m%d-%H%M%S") + '_待處理訂單資料整合檔.xlsx')
        _df_path=os.path.join(which_folder_to_save, _df_name)
        _df.to_excel(_df_path, index=False)
        # print('to_download_file 1', _df_path)

    def prepare_download_link(download_from_which_folder):
        # print('prepare_download_link 1')
        sorted_files_list = sorted(os.listdir(download_from_which_folder))
        # 應該給最新的檔案以供下載, 我們以檔案產生日期作為檔案前綴, 故找排序後最末的檔案即為該檔案
        # print('to_download_file 2', sorted_files_list)
        file_to_be_downloaded = sorted_files_list[-1]
        # print('to_download_file 2', file_to_be_downloaded)
        # print('to_download_file 2', os.path.join(download_from_which_folder, file_to_be_downloaded))
        file_to_be_downloaded = open(os.path.join(download_from_which_folder, file_to_be_downloaded), 'rb')
        return FileResponse(file_to_be_downloaded)
        
    print('making excel\n\n')
    write_current_pending_txns_to_excel_file(download_from_which_folder)
    # print('start 3')
    response = prepare_download_link(download_from_which_folder)
    return response


def download_search_file(request):
    download_folder = 'download_file'
    download_file=os.path.join(os.getcwd(),download_folder)
    sorted_files_list = sorted(os.listdir(download_file))
    print(sorted_files_list)
    file_download_name=''
    for sorted_file in sorted_files_list :
        if str(request.user) in sorted_file:
            try:
                os.remove(os.path.join(download_file,file_download_name))
            except:
                pass
            file_download_name=sorted_file
        else:
            try:
                os.remove(os.path.join(download_file,sorted_file))
            except:
                pass

    print(file_download_name)
    file_to_be_downloaded = open(os.path.join(download_file,file_download_name), 'rb')
    
    response = FileResponse(file_to_be_downloaded)
    return response


# 把這段function拉出來以便其他也可以
def wait_for_kashgari_model_done_parsing(signal='done_parsing'):
    is_done = 0
    while(is_done==0):
        ret = kash.stdout.readline().decode().strip()
        if not ret == 'done_parsing':
            continue
            # print('TEST_INFO', ret)
            # continue 讓while loop直接繼續下一個循環
        else:
            is_done = 1


#@login_required(login_url = '/accounts/login/')
def ordertracking(request):
    title = '訂單整合'

    upload_files_conditions = False
    is_integrated_done = False
    alicia = Alicia_0611.ALICIA()
    folder_where_are_uploaded_files_be = 'temp_files'
    folder_where_i_want_all_decrypted_files_be_at = 'order_manage/ALICIA/decrypt'

    alicia.raw_txns_dir = folder_where_i_want_all_decrypted_files_be_at
    alicia.decr_raw_txns_dir = folder_where_i_want_all_decrypted_files_be_at

    def clean_temp_files_in_folders(folder_list=[
                                                 folder_where_are_uploaded_files_be, 
                                                 folder_where_i_want_all_decrypted_files_be_at]):
        for _ in folder_list:
            alicia.delete_files_in_the_folder(_)

    if request.method == 'POST':
        st = time()
        if request.FILES.getlist("files"):
            # 先確認檔案符合條件
            alicia.wait_till_the_flag_comes_up(
                'all_flags/ordetracking_function_is_not_running.flag',
                'all_flags/ordetracking_function_is_running.flag')
            print('order_tracking: Change is not running to is running.')
            try:
                # 為了避免發生錯誤時,flag沒有被改回來
                clean_temp_files_in_folders()
                names_of_all_selected_files = [_.name for _ in request.FILES.getlist("files")]
                
                if_files_are_all_good, exception_files = \
                    alicia.check_if_all_files_are_good_for_ALICIA_pipeline(names_of_all_selected_files)
                
                if if_files_are_all_good:
                    # 所有檔案都符合條件, 進行存檔
                    fs = FileSystemStorage()
                    fs_backup = FileSystemStorage(location='user_uploaded_files/')
                    # 存放user上傳的訂單資料，以免未來需要做編輯 & 開發新功能
                    for each_file in request.FILES.getlist("files"):
                        print('each_file.name in VIEWS: ', each_file.name)
                        fs.save(each_file.name, each_file)
                        fs_backup.save(alicia.get_today("%Y%m%d-%H%M%S") + '_' + each_file.name, each_file)
                        # 上傳的檔案將被存放在預設為 '/HAPPYPI_0610_ANNIE/temp_files/' 的資料夾中
                        # 注意! 上傳的檔案包括「需要解密」的檔案跟「不需要解密」的檔案
                    print(f"上傳的檔案為：{os.listdir('user_uploaded_files')}")
                    alicia.move_files_and_decrypt_them(folder_where_are_uploaded_files_be, 
                                                    folder_where_i_want_all_decrypted_files_be_at)
                    print('Has Successfully Decrypted And Moved All Files.')
                else:
                    # 上傳的檔案有問題, 需要做例外控管!!!
                    upload_files_conditions = True
                    os.rename(
                            'all_flags/ordetracking_function_is_running.flag',
                            'all_flags/ordetracking_function_is_not_running.flag')
                    return render(request, 'order_manage/ordertracking.html', 
                                context={
                                            'upload_files_conditions': upload_files_conditions,
                                            'exception_files': exception_files,
                                            'is_integrated_done': is_integrated_done,
                                            'platforms_found': [],
                                            'platforms_not_found': [],
                                            'after_alicia_exception_files': []
                                    })
                # 前面都只是在清理
                # print('order_tracking info 1: Starts to integrate files.')
                platforms_found, platforms_not_found, after_alicia_exception_files = alicia._integrate_all_platforms()
                # print('alicia.aggregated_txns.shape', alicia.aggregated_txns.shape)
                # print('order_tracking info 2: Dobe integrating files.')
            
                is_integrated_done = True
                # 上面那行整合各平台交易資訊, 並回傳哪一些平台有找到, 哪一些沒有

                dataframe_after_parsing = None
                if alicia.aggregated_txns.shape[0] > 0:
                    # 當alicia.aggregated_txns長度不為0時再進行以下動作，
                    # 反之代表user只上傳了整合訂單檔案。
                    # 因為aggregated_txns只存放除了【整合訂單檔案】
                    print(f'brfore unique_ids_in_database_in_list {alicia.aggregated_txns.shape}')
                    unique_ids_in_database_in_list = list(History_data.objects.values_list('unique_id', flat=True))
                    print('order_tracking info 3: ', unique_ids_in_database_in_list[:10])
                    print(f'brfore to_split_old_unique_ids {alicia.aggregated_txns.shape}')
                    modified_unique_ids_in_database = alicia.to_split_old_unique_ids(unique_ids_in_database_in_list)
                    print('order_tracking info 4: ', modified_unique_ids_in_database[:10])

                    print(f'brfore pre_clean_raw_txns {alicia.aggregated_txns.shape}')
                    alicia.pre_clean_raw_txns(modified_unique_ids_in_database)
                    print('order_tracking info 5: ', alicia.aggregated_txns)
                    # alicia.aggregated_txns.to_excel('alicia.aggregated_txns.xlsx', index=False)
                    
                    
                    if alicia.aggregated_txns.shape[0] > 0:
                        prod_ipt = alicia.aggregated_txns.loc[:, '規格'].tolist()
                        # print(f'prod_ipt:  {prod_ipt}')   
                        num_ipt = alicia.aggregated_txns.loc[:, '數量'].astype(str).tolist()
                        result = kashgari_parsing(prod_ipt, num_ipt)
                        # print(f'result:  {result}')   
                        alicia.aggregated_txns.loc[:, '規格'] = np.array(result)
                        # print(f'before to_one_unique_id_df_after_kash: {alicia.aggregated_txns}')
                        # 在這裡先將官網2020青葉台菜年菜組合分離開來

                        official_qingye_txn_ids = alicia.aggregated_txns[
                            (alicia.aggregated_txns['通路'] == '樂天派官網') &
                            ((alicia.aggregated_txns['內容物'].str.contains('青葉臺菜')) | 
                            (alicia.aggregated_txns['內容物'].str.contains('青葉台菜')))]['訂單編號'].unique()
                        official_qingye_df = alicia.aggregated_txns[
                            alicia.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids)]
                        
                        alicia.aggregated_txns = \
                            alicia.aggregated_txns[~alicia.aggregated_txns['訂單編號'].isin(official_qingye_txn_ids)]

                        if alicia.aggregated_txns.shape[0]:
                            dataframe_after_parsing = alicia.to_one_unique_id_df_after_kash(alicia.aggregated_txns)
                        # print(f'after to_one_unique_id_df_after_kash: {dataframe_after_parsing}')
                        dataframe_after_parsing = pd.concat([official_qingye_df, alicia.aggregated_txns])
                        # print(f'official_qingye_df  {official_qingye_df}')
                        # print(f'aggregated_txns  {alicia.aggregated_txns}')
                        # print(f'dataframe_after_parsing  {dataframe_after_parsing}')
                        #except Exception as e:
                        #    print(f'something about qingye error: {e}')
                        dataframe_after_parsing = dataframe_after_parsing.drop(['unique_id'], axis=1)
                        dataframe_after_parsing['規格'] = \
                            dataframe_after_parsing['規格'].apply(alicia.aggregate_elements_in_subcontent)
                        # print(f'after_aggregate_elements_in_subcontent  {dataframe_after_parsing}')
                        alicia.remove_unique_id()
                        print('共花了', int(time()-st), '秒.', '\n分析了', dataframe_after_parsing.shape[0], '筆交易.')
                    print('共花了', int(time()-st), '秒.', '\n分析了 0 筆交易.')
                clean_temp_files_in_folders()
                # 先清理一下遺留的檔案
            except Exception as e:
                print('view_functions', e)
                os.rename(
                    'all_flags/ordetracking_function_is_running.flag',
                    'all_flags/ordetracking_function_is_not_running.flag')

            try:
                os.rename(
                        'all_flags/ordetracking_function_is_running.flag',
                        'all_flags/ordetracking_function_is_not_running.flag')
            except:
                pass
            # 將flag改回去讓下一個執行緒可以取用
            
            # 整理一下，確認有沒有df這個檔案，以及處理user上傳整合檔該怎麼寫進DB中的問題
            # print(df['規格'].tolist())
            # alicia.user_uploaded_aggregated_txns.to_excel('05_step5_user_uploaded.xlsx')
            # print(f'dataframe_after_parsing before last  {dataframe_after_parsing}')
            try:
                # print(f'before_combine_aggregated_txns_and_user_uploaded_aggregated_txns  {dataframe_after_parsing}')
                # print(f'before user_uploaded_aggregated_txns  {alicia.user_uploaded_aggregated_txns}')
                dataframe_after_parsing = alicia.combine_aggregated_txns_and_user_uploaded_aggregated_txns(
                    dataframe_after_parsing, alicia.user_uploaded_aggregated_txns)
                # print(f'after_combine_aggregated_txns_and_user_uploaded_aggregated_txns  {dataframe_after_parsing}')
                # print(f'after_combine_aggregated_txns_and_user_uploaded_aggregated_txns2  {dataframe_after_parsing.unique_id}')
            except Exception as e:
                print(f'error in combine_aggregated_txns_and_user_uploaded_aggregated_txns: {e}')
                exit()
                # dataframe_after_parsing指的是user從各個平台下載下來，經過Alicia整理後的原始訂單資料，
                # user_uploaded_aggregated_txns則是之前已經整理過而產出的訂單整合檔再上傳

            # print(f'before model tools: {dataframe_after_parsing}')
            # 將整理好的資料寫進資料庫
            print('將整理好的資料寫進資料庫...')
            if dataframe_after_parsing.shape[0] > 0:
                model_writer = HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=dataframe_after_parsing)
                model_writer.write_in_2diff_db()
                model_writer.qingye_cleaning()

            # write_current_pending_txns_to_excel_file()
            # 似乎不需要在這裡就將資料寫出, 可以等待user按了下載再產出最新檔案就好
            print('VIEWS HAS TAKEN(S) : ', time() - st)
            
            


            return render(request, 'order_manage/ordertracking.html', 
                      locals())

    else:
        return render(request, 'order_manage/ordertracking.html', 
                      locals())



clean_num = lambda x: 1 if len(x)==0 else int(x)
now_time = lambda : '-'.join([str(_) for _ in localtime()[:3]]) + ' ' + ':'.join([str(_) for _ in localtime()[3:6]])

# these are for getting value from dictionary in templates by its key-value
from django.template.defaulttags import register
@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)
# these are for getting value from dictionary in templates by its key-value

# Create your views here.
def abstract_func(request):
    title = '規格/數量抓取測試'
    if request.method == 'POST':
        print('abstract_func1', 'Get Post')
        inputs_prod, inputs_num, outputs = {}, {}, {}
        # 全都以1, 2, 3 in string作為key value
        inputs_prod_in_array, inputs_num_in_array, outputs_in_array = [], [], []

        for input_index in ['1', '2', '3']:
            if len(request.POST['prod_input' + input_index].strip()):
                inputs_prod[input_index] = request.POST['prod_input' + input_index].strip()
                inputs_num[input_index] = clean_num(request.POST['num_input' + input_index].strip())

                inputs_prod_in_array.append(inputs_prod[input_index])
                inputs_num_in_array.append(inputs_num[input_index])

        print('abstract_func2', inputs_prod_in_array, inputs_num_in_array)
        outputs_in_array = kashgari_parsing(inputs_prod_in_array, inputs_num_in_array)

        for i, each_key in enumerate(inputs_prod.keys()):
            outputs[each_key] = outputs_in_array[i]
            with open('order_manage/what_hp_input.txt', 'a') as w:
                w.write(now_time() + '||' + inputs_prod[each_key] + '||' + \
                    str(inputs_num[each_key]) + '||' + outputs_in_array[i] + '\n')

        return render(request, "order_manage/abstract_func.html", locals())
 
    return render(request, "order_manage/abstract_func.html", locals())


def redirect_2_shipping_url(request):
    # http://127.0.0.1:8000/order_manage/edo_url/?shipping_number=6659750403&logistic_company=xinzhu
    # http://127.0.0.1:8000/order_manage/edo_url/?shipping_number=906395037950&logistic_company=black_cat
    if 'shipping_number' in request.GET and 'logistic_company' in request.GET:
        the_url = generate_shipping_url(
            shipping_number=request.GET['shipping_number'], 
            logistic_company=request.GET['logistic_company'])
        response = redirect(the_url)
        return response
    else:
        return HttpResponse('請確認連結是否有誤唷!')












def import_selfmade_txns(request):
    title = '匯入自訂訂單'

    if request.method == 'POST':
        alicia = Alicia_0611.ALICIA()
        folder_where_are_uploaded_files_be = 'temp_files'
        alicia.delete_files_in_the_folder(folder_where_are_uploaded_files_be)  # 清理舊資料

        names_of_all_selected_files = [_.name for _ in request.FILES.getlist("files")]
        
        if_files_are_all_good, exception_files = \
            alicia.check_if_all_files_are_good_for_ALICIA_pipeline(names_of_all_selected_files)
        
        if if_files_are_all_good:
            # 所有檔案都符合條件，先做呈現後，再考慮寫入
            # 匯入檔案至少要包含以下欄位:
                #通路
                #抓單日
                #訂單編號
                #訂購人
                #收件人
                #地址
                #手機
                #內容物
                #數量
                #備註
                #宅單
            must_columns = ['通路', '抓單日', '訂單編號', '訂購人', '收件人', '地址',
                '手機', '內容物', '數量', '備註', '宅單', '規格']
            df = pd.DataFrame(columns=must_columns)
            try:
                fs = FileSystemStorage()
                for each_file in request.FILES.getlist("files"):
                    fs.save(each_file.name, each_file)
                    print('each_file.name in VIEWS: ', each_file.name)
                    temp_df = pd.read_excel(os.path.join(folder_where_are_uploaded_files_be, each_file.name))
                    for each_column in must_columns:
                        assert each_column in temp_df.columns
                        # 檢查匯入的資料中是否含有這些必要欄位
                    df = pd.concat([df, temp_df], join='inner')
                    os.unlink(os.path.join(folder_where_are_uploaded_files_be, each_file.name))
                # 到這裡為止，資料已經儲存在記憶體中了
                df = df.sort_values(by=['抓單日', '訂單編號']).reset_index(drop=True)
            except Exception as e:
                print(e)
            
            def fill_df_with_alicia_full_columns(target_df):
                print(target_df.shape)
                for each_column in alicia.aggregated_txns.columns:
                    if each_column not in target_df.columns:
                        target_df.insert(0, each_column, ['' for _ in range(target_df.shape[0])])
                return target_df

            def cleansing(target):
                if pd.isnull(target):
                    return ''
                else:
                    target = str(target)
                
                if target.strip() == '':
                    return target
                else:
                    return target.strip().replace('-', '').replace('\'', '').replace('\n', ' ')

            def try_to_generate_shipping_url_from_shipping_id(target_shipping_id):
                has_content = 1
                if pd.isnull(target_shipping_id):
                    has_content = 0
                if str(target_shipping_id).strip() == '' or len(str(target_shipping_id).strip()) == 0:
                    has_content = 0

                _temp_logistic_company = None
                if has_content == 1:
                    if len(target_shipping_id) == 10:
                        # 新竹物流的貨運編號長度為10，黑貓的長度為12
                        _temp_logistic_company = 'xinzhu'
                    elif len(target_shipping_id) == 12:
                        _temp_logistic_company = 'black_cat'
                    
                    if _temp_logistic_company is not None:
                        return 'http://61.222.157.151/order_manage/edo_url/?shipping_number=' + str(target_shipping_id) + '&logistic_company=' + _temp_logistic_company
                return ''

            for each_column in must_columns:
                if each_column not in ['內容物', '規格', '訂單編號']:
                    try:
                        df[each_column] = df[each_column].apply(cleansing)
                    except Exception as e:
                        print(e)

            df['手機'] = df['手機'].apply(alicia.make_phone_and_mobile_number_clean)
            df['貨運連結'] = df['宅單'].apply(try_to_generate_shipping_url_from_shipping_id)
            
            df = alicia._clean_dataframe(df, make_null_be_nullstring=True)
            
            alicia.aggregated_txns = fill_df_with_alicia_full_columns(df)
            alicia.pre_clean_raw_txns()
            

            df = alicia.to_one_unique_id_df_after_kash(alicia.aggregated_txns)
            df = df.drop(['unique_id'], axis=1)
            alicia.remove_unique_id()

            model_writer = HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=df)
            model_writer.write_in_2diff_db()
            # alicia.aggregated_txns = pd.concat([alicia.aggregated_txns, df])
            # df.to_excel('xxx.xlsx', index=False)
            # print('Has Successfully Decrypted And Moved All Files.')

        return render(request, 'order_manage/import_selfmade.html', locals())
    else:
        return render(request, 'order_manage/import_selfmade.html', locals())


class history_data(View):
    download_folder = 'download_file'
    
    def get(self, request):

        data= History_data.objects.filter(id=0)
        if 'platform' in request.GET:
            data = History_data.objects.all()

            if request.GET['platform'] is not '':
                data=data.filter(platform=request.GET['platform'])
                print(data)

            if request.GET["txn_id"] is not '':
                data=data.filter(txn_id=request.GET["txn_id"])
                print(data)

            if request.GET["customer_name"] is not '':
                data=data.filter(customer_name=request.GET["customer_name"])
                print(data)

            if request.GET["receiver_name"] is not '':
                data=data.filter(receiver_name=request.GET["receiver_name"])
                print(data)

            if request.GET["receiver_phone_nbr"] is not '':
                data=data.filter(receiver_phone_nbr=request.GET["receiver_phone_nbr"])
                print(data)

            if request.GET["receiver_mobile"] is not '':
                data=data.filter(receiver_mobile=request.GET["receiver_mobile"])
                print(data)

            if request.GET["receiver_address"] is not '':
                data=data.filter(receiver_address=request.GET["receiver_address"])
                print(data)
                #內容篩選未完成
            if request.GET["content"] is not '':
                data=data.filter(content=request.GET["content"])
                print(data)

            if request.GET["how_many"] is not '':
                data=data.filter(how_many=int(request.GET["how_many"]))
                print(data)
            #金額篩選未完成
            #if request.GET["how_much_min"] is not '':
            #    data=data.filter(how_much=int(request.GET["how_much_min"]))
            #    print(data)

            #if request.GET["how_much_max"] is not '':
            #    data=data.filter(how_much=int(request.GET["how_much_max"]))
            #    print(data)

            if request.GET["remark"] is not '':
                data=data.filter(remark=request.GET["remark"])
                print(data)

            if request.GET["shipping_id"] is not '':
                data=data.filter(shipping_id=request.GET["shipping_id"])
                print(data)

            if request.GET["subcontent"] is not '':
                data=data.filter(subcontent=request.GET["subcontent"])
                print(data)

            if request.GET["ifsend"] is not '':
                data=data.filter(ifsend=request.GET["ifsend"])
                print(data)

            if request.GET["ifcancel"] is not '':
                data=data.filter(ifcancel=request.GET["ifcancel"])
                print(data)
                
            if (request.GET["from_date"] is not '') & (request.GET["to_date"] is not ''):
                from_day = request.GET["from_date"].split("/")[1]
                from_month = request.GET["from_date"].split("/")[0]
                from_year = request.GET["from_date"].split("/")[2]
                from_date=from_year+'-'+from_month+'-'+from_day

                to_day = request.GET["to_date"].split("/")[1]
                to_month = request.GET["to_date"].split("/")[0]
                to_year = request.GET["to_date"].split("/")[2]
                to_date=to_year+'-'+to_month+'-'+to_day
                print(from_date,to_date)
                data=data.filter(file_created_date__range=[from_date,to_date])
                
            file_name=datetime.today().strftime("%Y%m%d-%H%M%S") +str(request.user)+ '_歷史查詢訂單資料整合檔.xlsx'
            download_file=os.path.join(os.getcwd(),self.download_folder,file_name)
            df = read_frame(data) 
            
            df_translate=HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=df.iloc[:,1:])
            df_translate.english_db_column_names_to_mandarin()
            df=df_translate.dataframe
            print('create file'+file_name)
            df.to_excel(download_file, index=False)
            
            return render(request, 'order_manage/orderhistory.html',{'data':data})

        return render(request, 'order_manage/orderhistory.html',{'data':data})

    
    def post(self, request):
            
            data = History_data.objects.all()
            
            if 'ifsend,1' in request.POST:
                df=read_frame(History_data.objects.filter(id=0))
                #計算筆數
                number=int((len(request.POST)+1)/2)
                st = time()
                for n in range(1,number):
                    #抓出ID
                    ifsend_data_id=request.POST['ifsend,'+str(n)].split(",")[0]
                    print(ifsend_data_id)
                    ifcancel_data_id=request.POST['ifcancel,'+str(n)].split(",")[0]
                    #抓出VALUE
                    ifsend_data_val=request.POST['ifsend,'+str(n)].split(",")[1]
                    ifcancel_data_val=request.POST['ifcancel,'+str(n)].split(",")[1]
                    #更新DATA
                    #if (History_data.objects.filter(id =ifsend_data_id).('ifsend')) == ifsend_data_val:
                    History_data.objects.filter(id =ifsend_data_id).update(ifsend=ifsend_data_val)
                    #if (History_data.objects.filter(id =ifcancel_data_id).('ifcancel')) == ifcancel_data_val:
                    History_data.objects.filter(id =ifcancel_data_id).update(ifcancel=ifcancel_data_val)
                    
                    df_add=read_frame(History_data.objects.filter(id=ifsend_data_id))
                    df=df.append(df_add, ignore_index = True)

                file_name=datetime.today().strftime("%Y%m%d-%H%M%S")+str(request.user) + '_歷史查詢訂單資料整合檔.xlsx'
                download_file=os.path.join(os.getcwd(),self.download_folder,file_name)
                df_translate=HISTORY_DATA_and_Subcontent_user_edit_record_db_writer(dataframe=df.iloc[:,1:])
                df_translate.english_db_column_names_to_mandarin()
                df=df_translate.dataframe
                print('create file'+file_name)
                df.to_excel(download_file, index=False)

                print('共花了', int(time()-st), '秒.')
                return HttpResponse(status=204)

            else:
                return render(request, 'order_manage/orderhistory.html',{'data':data})
'''# dataframe_after_parsing.to_excel('ssssss.xlsx', index=False)
            # 20210109 進行青葉臺菜的年菜整合 >>
            #   只要是相同的 訂單編號，假設裡面有a, b, c, d 四筆訂單，
            #   如果任何一筆含有「青葉臺菜」，就把這四筆歸為同一筆訂單。
            txn_id_with_qingye_in_list = \
                dataframe_after_parsing[dataframe_after_parsing['內容物'].str.contains('青葉臺菜')]['訂單編號'].tolist()
            # print('so far so good1')
            txn_id_with_qingye_in_list = list(set(txn_id_with_qingye_in_list))
            # print('so far so good2')
            txns_with_qingye_df = \
                dataframe_after_parsing[dataframe_after_parsing['訂單編號'].isin(txn_id_with_qingye_in_list)]
            # print('so far so good3')
            df_without_qingye = \
                dataframe_after_parsing[~dataframe_after_parsing['訂單編號'].isin(txn_id_with_qingye_in_list)]
            # print('so far so good4')
            # print(f'txn_id_with_qingye_in_list len: {len(txn_id_with_qingye_in_list)}')
            # 接下來進行合併
            for each_txn_id_with_qingye in txn_id_with_qingye_in_list:
                # print(f'current position: {txn_id_with_qingye_in_list.index(each_txn_id_with_qingye)}')
                try:
                    tdf = txns_with_qingye_df[txns_with_qingye_df['訂單編號']==each_txn_id_with_qingye].reset_index(drop=True)
                    # print('test 1')
                    _temp_df = pd.DataFrame(columns=txns_with_qingye_df.columns)
                    # print(f'test 2 {_temp_df.shape[0]}')
                    _temp_df.loc[_temp_df.shape[0]] = tdf.iloc[0]  # 先把第一行全部搬過去
                    # print(f'test 3 {_temp_df.shape[0]}')
                    _temp_df.loc[_temp_df.shape[0] - 1, '內容物'] = \
                        ', '.join(sorted((tdf['內容物'] + ' * ' + tdf['數量'].astype(str)).tolist()))
                    # print('test 4')
                    _temp_df.loc[_temp_df.shape[0] - 1, '規格'] = ', '.join(tdf['規格'].tolist())
                    # print('test 5')
                    _temp_df.loc[_temp_df.shape[0] - 1, '供應商'] = '青葉'
                    # print('test 6')
                    #_temp_df.loc[_temp_df.shape[0] - 1, 'unique_id'] = \
                    #    _temp_df.loc[_temp_df.shape[0] - 1, 'unique_id'].split('|')[0] + '|' + '青葉' + '|' + _temp_df.loc[_temp_df.shape[0] - 1, 'unique_id'].split('|')[2]
                    #print(f"xxxx {_temp_df.loc[_temp_df.shape[0] - 1, 'unique_id']}")
                    # print('test 7')
                    df_without_qingye = \
                        pd.concat([df_without_qingye, _temp_df]).reset_index(drop=True)
                    # print(df_without_qingye.head(1).T)
                except Exception as e:
                    print(f'qingye part Exception: {e}')
                    break
            dataframe_after_parsing = df_without_qingye.sort_values('訂單編號')
            dataframe_after_parsing = df_without_qingye.sort_values('供應商').reset_index(drop=True)'''
