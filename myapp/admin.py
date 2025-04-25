from django.contrib import admin
from .models import WbLk, Groups, User, Price


class PriceAdmin(admin.ModelAdmin):
    list_display = ('get_lk_name', 'nmid', 'vendorcode')  # Определяет, какие поля будут отображаться в списке
    search_fields = ('lk__name', 'nmid', 'vendorcode') # Поля для поиска
    ordering = ('nmid',)  # Сортировка по умолчанию
    list_filter = ('lk',)  # Фильтр по полю 'lk'

    def get_lk_name(self, obj):
        return obj.lk.name

    get_lk_name.short_description = 'Личный кабинет'

admin.site.register(WbLk)
admin.site.register(Groups)
admin.site.register(User)
admin.site.register(Price, PriceAdmin)