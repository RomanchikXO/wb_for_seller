from django.urls import path
from . import views

urlpatterns = [
    path('', views.main_view, name='main'),
    path('repricer/', views.repricer_view, name='repricer'),
    path('repricer/save/', views.repricer_save, name='repricer_save'),
    path('podsort/', views.podsort_view, name='podsort'),
    path('repricer/export_excel/', views.export_excel, name='export_excel'),
    path('repricer/upload_excel/', views.upload_excel, name='upload_excel'),
    path('podsort/export_excel_podsort/', views.export_excel_podsort, name='export_excel_podsort'),
    path('margin/', views.margin_view, name='margin'),
    path('margin/api/margin-data/', views.get_margin_data, name='margin-data'),
    path('shipments/', views.shipment_view, name='shipments'),
    path('warehousewb/', views.warehousewb_view, name='warehousewb'),
]
