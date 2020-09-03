from django.urls import path
from django.conf.urls import include, url
from . import views

app_name ='abstract_fun'

urlpatterns = [
    url(r'^fun1', views.abstract_fun, name = 'fun'),
] 
