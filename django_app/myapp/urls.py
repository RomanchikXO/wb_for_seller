from django.urls import path

from myapp.views import myapp_index

pp_name = 'myapp'  #чтобы файл был действительно отдельным пространством имен


urlpatterns = [
    path('', myapp_index, name='index'),
]