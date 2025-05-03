from django.urls import path
from . import views

urlpatterns = [
    path('', views.main_view, name='main'),
    path('repricer/', views.repricer_view, name='repricer'),
]
