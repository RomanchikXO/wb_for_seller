from django.urls import path
from . import views

urlpatterns = [
    path('', views.main_view, name='main'),
    path('restart-container/<str:container_id>/', views.restart_container_view, name='restart-container'),
    path('stop-container/<str:container_id>/', views.stop_container_view, name='stop-container'),
    path('repricer/', views.repricer_view, name='repricer'),
    path('repricer/save/', views.repricer_save, name='repricer_save'),
    path('repricer/get_marg/', views.get_marg_api, name='get_marg_api'),
    path('podsort/', views.podsort_view, name='podsort'),
    path('repricer/export_excel/', views.export_excel, name='export_excel'),
    path('repricer/upload_excel/', views.upload_excel, name='upload_excel'),
    path('podsort/export_excel_podsort/', views.export_excel_podsort, name='export_excel_podsort'),
    path('podsort/set_tags/', views.set_tags, name='set_tags'),
    path('podsort/add_tag/', views.add_tag, name='add_tag'),
    path('margin/', views.margin_view, name='margin'),
    path('margin/api/margin-data/', views.get_margin_data, name='margin-data'),
    path('shipments/', views.shipment_view, name='shipments'),
    path('warehousewb/', views.warehousewb_view, name='warehousewb'),
    path('warehousewb/api/warehousewb-data/', views.get_warehousewb_data, name='warehousewb-data'),
    path('warehousewb/api/add-data/', views.get_warehousewb_add_data, name='warehousewb-add-data'),
    path('warehousewb/api/submit-supply/', views.warehousewb_submit_supply, name='warehousewb-submit-supply'),
    path('api/google-webhook/', views.google_webhook_view, name='google_webhook'),
]
