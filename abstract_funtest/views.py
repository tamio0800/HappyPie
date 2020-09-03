from django.shortcuts import render
from django.contrib.auth.decorators import login_required


@login_required(login_url = '/accounts/login/')
def abstract_fun(request):
    return render(request, 'abstract_tem/abstract_funtest.html')