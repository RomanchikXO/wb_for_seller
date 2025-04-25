from django.db import models
import json

# Модель для таблицы wb_lk
class WbLk(models.Model):
    group = models.IntegerField()
    name = models.CharField(max_length=255)
    token = models.CharField(max_length=400)

    def __str__(self):
        return self.name

# Модель для таблицы group
class Group(models.Model):
    name = models.CharField(max_length=255)
    permissions = models.JSONField()  # Используем JSONField для хранения списка разрешений

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
    nmID = models.IntegerField()
    vendorCode = models.CharField(max_length=255)
    sizes = models.JSONField()  # Массив JSON для хранения размеров
    discount = models.IntegerField()
    clubDiscount = models.IntegerField()
    editableSizePrice = models.IntegerField()

    def __str__(self):
        return f"{self.vendorCode} - {self.nmID}"
