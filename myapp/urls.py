from django.urls import path

from myapp.views import register_view, login_view

pp_name = 'myapp'  #чтобы файл был действительно отдельным пространством имен


urlpatterns = [
    path('', register_view, name='register'),
    path('login/', login_view, name='login'),
]