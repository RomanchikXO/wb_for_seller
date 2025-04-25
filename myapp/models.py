from django.db import models
import json


# Модель для таблицы group
class Groups(models.Model):
    name = models.CharField(max_length=255)
    permissions = models.JSONField()  # Используем JSONField для хранения списка разрешений

    def __str__(self):
        return self.name

# Модель для таблицы wb_lk
class WbLk(models.Model):
    # myapp_wblk
    groups = models.ForeignKey(Groups, on_delete=models.CASCADE, null=True) #groups_id_id в бд
    name = models.CharField(max_length=255)
    token = models.CharField(max_length=400)

    def __str__(self):
        return self.name

# Модель для таблицы users
class User(models.Model):
    name = models.CharField(max_length=255)
    password = models.CharField(max_length=255)

    def __str__(self):
        return self.name

# Модель для таблицы prices
class Price(models.Model):
    # myapp_price
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1) #lk_id в бд
    nmid = models.IntegerField(default=0)
    vendorcode = models.CharField(max_length=255, default=0)
    sizes = models.JSONField()  # Массив JSON для хранения размеров
    discount = models.IntegerField()
    clubdiscount = models.IntegerField(default=0)
    editablesizeprice = models.IntegerField(default=0)

    class Meta:
        unique_together = ['nmid', 'lk']  # Уникальное ограничение на комбинацию nmID и lk

    def __str__(self):
        return f"{self.vendorCode} - {self.nmID}"
