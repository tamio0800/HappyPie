from django.shortcuts import render, redirect
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from django.contrib.auth import login, logout
from django.contrib.auth.decorators import login_required
from django.contrib import auth
from django.contrib.auth.password_validation import validate_password
from django.contrib.auth.models import User
# 註冊頁

@login_required(login_url = '/accounts/login/')
def signup_view(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        #print(n_username)
        password1 = request.POST.get('password1')
        password2 = request.POST.get('password2')
        
        
        if User.objects.filter(username = username).exists():
            return render(request, 'accounts/signup.html',
        {'mystery' : '此帳號已經註冊過了',
        'title' : '註冊頁'})
        elif password1 != password2:
            return render(request, 'accounts/signup.html',
        {'mystery' : '兩次輸入的密碼不同',
        'title' : '註冊頁'})

        else:
            user = User.objects.create_user(username = username,
                                 password = password1)
            user.save()
            login(request, user)
            return redirect('home') # url name
    # if is GET request
    else:
        return render(request, 'accounts/signup.html')
    # ex: when password doesn't match
    return render (request, 'accounts/signup.html', 
    {'form' : form, 'title' : '註冊頁'})


'''
# 註冊頁
@login_required(login_url = '/accounts/login/')
def signup_view(request):
    if request.method == 'POST':
        print(request.POST)
        form = UserCreationForm(request.POST)
        #print(form)
        # T or F
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect('home') # url name
    # if is GET request
    else:
        print('something wrong')
        #form = UserCreationForm()
    # ex: when password doesn't match
    return render (request, 'accounts/signup.html', 
    {'form' : form, 'title' : '註冊頁'})
'''


# 登出
def logout_view(request):
    logout(request)
    return redirect("accounts:login")

# 登入
def login_view(request):
    if request.method == 'POST':
        #print(request.POST['inputEmail'])
        #print(request.POST['inputPassword'])
        form = AuthenticationForm(data = request.POST)
        #print(form)

        username = request.POST.get('id_username')
        password = request.POST.get('id_password')
        user = auth.authenticate(username=username, password=password)
        
        if user is not None and user.is_active:
            login(request, user)
            return redirect('home') # url name
        else :
            return render (request, 'accounts/loginpage.html', 
            {'mystery' : '帳號或密碼錯誤', 'title' : '登入頁'})

    # if GET request
    #else:
    #    form = AuthenticationForm()
    # ex: when password doesn't match
    return render (request, 'accounts/loginpage.html', 
    )


'''    
def login_view(request):

    if request.method == 'POST':
        form = AuthenticationForm(data = request.POST)
        if form.is_valid():
            # login user
            user = form.get_user()
            login(request, user)
            return redirect('home') # url name

    # if GET request
    else:
        form = AuthenticationForm()
    # ex: when password doesn't match
    return render (request, 'accounts/loginpage.html', 
    {'form' : form})'''