from django.urls import path
from django.conf.urls import url
from . import views
from django.contrib.auth.decorators import login_required

urlpatterns = [
    # 正式上傳要合併訂單檔案的地方
    url(r'^ordertracking/$', views.ordertracking,name='ordertracking'),
    url(r'^orderhistory/$', login_required(login_url = '/accounts/login/')(views.history_data.as_view())),
    path('to_download_file/', views.to_download_file, name='downloadfile'),
    path('download_search_file/', views.download_search_file, name='downloadsearchfile'),
    # 新加simple upload的頁面以便進行整合測試 by tamio@2020.06.14
    # 將該url改成複製ordertracking函式, 在這裡先測試完新增功能確認無誤後, 再回頭更新ordertracking  by tamio@2020.06.19
    url(r'^name_test/$', views.abstract_func),  # 這個是for 規格抓取測試
    url(r'^edo_url/$', views.redirect_2_shipping_url),
    url(r'^import_selfmade/$', views.import_selfmade_txns),  # 這個是for user自己整理的資料上傳
] 

# http://127.0.0.1:8000/order_manage/upload_test