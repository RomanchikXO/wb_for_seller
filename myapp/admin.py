from django.contrib import admin
from .models import WbLk, Groups, CustomUser, Price, CeleryLog, nmids, Stocks, Orders, Repricer, Questions


class QuestionsAdmin(admin.ModelAdmin):
    list_display = ('nmid', 'id_question', 'created_at', 'question')
    search_fields = ('nmid', 'id_question')
    ordering = ('created_at',)
    list_filter = ('is_answered',)


class PriceAdmin(admin.ModelAdmin):
    list_display = ('get_lk_name', 'nmid', 'vendorcode', 'updated_at', 'spp', 'blackprice')  # Определяет, какие поля будут отображаться в списке
    search_fields = ('lk__name', 'nmid', 'vendorcode') # Поля для поиска
    ordering = ('updated_at',)  # Сортировка по умолчанию
    list_filter = ('lk',)  # Фильтр по полю 'lk'

    def get_lk_name(self, obj):
        return obj.lk.name

    get_lk_name.short_description = 'Личный кабинет'

class CeleryLogAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'source', 'level', 'message')
    list_filter = ('level', 'source', 'timestamp')
    search_fields = ('message',)

class NmidsAdmin(admin.ModelAdmin):
    list_display = (
        'nmid', 'title', 'brand', 'vendorcode', 'subjectname', 'needkiz',
        'lk', 'created_at', 'updated_at', 'added_db'
    )
    list_filter = ('brand', 'subjectname', 'needkiz', 'lk')
    search_fields = ('nmid', 'vendorcode', 'title', 'brand', 'nmuuid')
    ordering = ('-added_db',)
    date_hierarchy = 'added_db'

class StocksAdmin(admin.ModelAdmin):
    list_display = (
        'supplierarticle', 'nmid', 'barcode',
        'quantity', 'inwaytoclient', 'inwayfromclient',
        'quantityfull', 'warehousename', 'lastchangedate',
        'isrealization',
    )
    list_filter = ('warehousename', 'issupply', 'isrealization')
    search_fields = ('supplierarticle', 'barcode', 'nmid')
    ordering = ('-lastchangedate',)

class OrdersAdmin(admin.ModelAdmin):
    list_display = (
        'date', 'lastchangedate', 'supplierarticle',
        'nmid', 'barcode', 'warehousename', 'countryname',
        'brand', 'totalprice', 'finishedprice', 'iscancel'
    )
    list_filter = ('lk', 'iscancel', 'warehousename', 'brand', 'countryname', 'isrealization', 'issupply')
    search_fields = ('supplierarticle', 'nmid', 'barcode', 'gnumber', 'srid')
    ordering = ('-date',)

class RepricerAdmin(admin.ModelAdmin):
    list_display = (
        'nmid', 'keep_price', 'is_active'
    )

    list_filter = ('lk', 'is_active')
    search_fields = ('nmid',)
    ordering = ('is_active',)

admin.site.register(Repricer, RepricerAdmin)
admin.site.register(Orders, OrdersAdmin)
admin.site.register(Stocks, StocksAdmin)
admin.site.register(nmids, NmidsAdmin)
admin.site.register(CeleryLog, CeleryLogAdmin)
admin.site.register(WbLk)
admin.site.register(Groups)
admin.site.register(CustomUser)
admin.site.register(Price, PriceAdmin)
admin.site.register(Questions, QuestionsAdmin)