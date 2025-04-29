from django.contrib import admin
from .models import WbLk, Groups, User, Price, CeleryLog, nmids, Stocks


class PriceAdmin(admin.ModelAdmin):
    list_display = ('get_lk_name', 'nmid', 'vendorcode', 'updated_at')  # Определяет, какие поля будут отображаться в списке
    search_fields = ('lk__name', 'nmid', 'vendorcode') # Поля для поиска
    ordering = ('updated_at',)  # Сортировка по умолчанию
    list_filter = ('lk',)  # Фильтр по полю 'lk'

    def get_lk_name(self, obj):
        return obj.lk.name

    get_lk_name.short_description = 'Личный кабинет'

class CeleryLogAdmin(admin.ModelAdmin):
    list_display = ('timestamp', 'level', 'message')
    list_filter = ('level', 'timestamp')
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
        'supplierarticle', 'techsize', 'nmid', 'barcode',
        'quantity', 'inwaytoclient', 'inwayfromclient',
        'quantityfull', 'warehousename', 'lastchangedate',
        'issupply', 'isrealization',
    )
    list_filter = ('warehousename', 'techsize', 'issupply', 'isrealization')
    search_fields = ('supplierarticle', 'barcode', 'nmid')
    ordering = ('-lastchangedate',)


admin.site.register(Stocks, StocksAdmin)
admin.site.register(nmids, NmidsAdmin)
admin.site.register(CeleryLog, CeleryLogAdmin)
admin.site.register(WbLk)
admin.site.register(Groups)
admin.site.register(User)
admin.site.register(Price, PriceAdmin)