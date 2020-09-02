from django.http import HttpResponse
from django.shortcuts import render
from django.contrib import admin
from django.contrib.auth import login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.template import RequestContext



#首頁
@login_required(login_url = '/accounts/login/')
def homepage(request):
    # 指定某些帳號群組才能看到網頁部分內容
    user = request.user # 當前登入的user
    g_val = request.user.groups.values_list('name',flat = True) # QuerySet Object
    pyli = list(g_val) # user 屬於哪些group
    # 如果 user 加入的 group中有符合有權限的群組
    if "happy_manager" in pyli:
        return render(request, 'homepage.html', 
        {'title':'首頁',
        'Auth':'happy_manager'})
    else:
        return render(request, 'homepage.html', {'title':'首頁'})

# 登入頁
def login_fun(request):
    title = '登入'
    acct = ''
    if request.method == 'POST':
        acct = request.POST['inputEmail']
        pw = request.POST['inputPassword']

    return render(request, 'loginpage.html', locals())

# 訂單整合功能頁
@login_required(login_url = '/accounts/login/')
def ordertracking(request):
    return render(request, 'ordertracking.html',{'title':'訂單整合'})

# 歷史訂單查詢與編輯
@login_required(login_url = '/accounts/login/')
def orderhistory(request):
    return render(request, 'orderhistory.html',{
        'name' : "合併訂單",
        'title': "歷史訂單查詢結果"
    })

@login_required(login_url = '/accounts/login/')
def base_layout(request):
    return render(request, 'base_layout.html')


def test_p(request):
    return render(request, 'test_2.html')

#def handler404(request):
#    title = '找不到頁面'
#    response = render(request, '404-error-page.html', locals())
#    response.status_code = 404
#    return response
     
    