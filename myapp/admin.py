from django.contrib import admin
from .models import WbLk, Groups, User, Price, CeleryLog


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

admin.site.register(CeleryLog, CeleryLogAdmin)
admin.site.register(WbLk)
admin.site.register(Groups)
admin.site.register(User)
admin.site.register(Price, PriceAdmin)