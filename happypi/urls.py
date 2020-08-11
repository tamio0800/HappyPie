from django.contrib import admin
from django.urls import path
from django.conf.urls import include, url
from . import views
from django.contrib.staticfiles.urls import staticfiles_urlpatterns


urlpatterns = [
    url(r'^manage_backend/', admin.site.urls),
    url(r'^homepage/$', views.homepage, name = 'home'),
    url(r'^order_manage/', include('order_manage.urls')),
    #url(r'^orderhistory/$', include('order_manage.urls')),
    url(r'^base_layout/$', views.base_layout),
    #url(r'^abstract_funtest/', include('abstract_funtest.urls')),
    url(r'^accounts/', include('accounts.urls')),
    url(r'^test/', views.test_p),
    #url(r'^404/', views.handler404)
] 

urlpatterns += staticfiles_urlpatterns()
#urlpatterns += [
#    path('accounts2/', include('django.contrib.auth.urls'))
#]