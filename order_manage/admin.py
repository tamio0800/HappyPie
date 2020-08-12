from django.contrib import admin
from .models import History_data #  python manage.py makemigrations

# Register your models here.
admin.site.register(History_data)