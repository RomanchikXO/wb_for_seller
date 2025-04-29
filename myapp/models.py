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
    groups = models.ForeignKey(Groups, on_delete=models.CASCADE, null=True) #groups_id в бд
    name = models.CharField(max_length=255)
    token = models.CharField(max_length=400)

    class Meta:
        verbose_name = "Личный кабинет"
        verbose_name_plural = "Личные кабинеты"
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
    updated_at = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        unique_together = ['nmid', 'lk']  # Уникальное ограничение на комбинацию nmID и lk

    def __str__(self):
        return f"{self.vendorcode} - {self.nmid}"


class nmids(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1) #lk_id в бд
    nmid = models.IntegerField() # Артикул WB
    imtid = models.IntegerField() # ID карточки товара. Артикулы WB из одной карточки товара будут иметь одинаковый imtID
    nmuuid = models.CharField(max_length=255) # Внутренний технический ID товара
    subjectid = models.IntegerField() # ID предмета
    subjectname = models.CharField(max_length=255) # Название предмета
    vendorcode = models.CharField(max_length=255) # Артикул продавца
    brand = models.CharField(max_length=255) # Бренд
    title = models.CharField(max_length=500) # Наименование товара
    description = models.TextField() # Описание товара
    needkiz = models.BooleanField() # Требуется ли код маркировки для этого товара
    dimensions = models.JSONField() # Габариты и вес товара c упаковкой, см и кг
    characteristics = models.JSONField() # Характеристики
    sizes = models.JSONField() # Размеры товара
    created_at = models.DateTimeField() # Дата создания карточки товара (по данным WB)
    updated_at = models.DateTimeField() # Дата изменения карточки товара (по данным WB)
    added_db = models.DateTimeField(auto_now_add=True) # по UTC

    class Meta:
        unique_together = ['nmid', 'lk']
        verbose_name = "Товар WB"
        verbose_name_plural = "Товары WB"

    def __str__(self):
        return f"{self.nmid} – {self.title} ({self.brand})"


# class Stocks(models.Model):
#     lastChangeDate = models.DateTimeField()
#     warehouseName = models.CharField(max_length=255, default=0)
#     supplierArticle = models.CharField(max_length=255, default=0)
#     nmid = models.IntegerField(default=0)
#     barcode = models.IntegerField(default=0)
#     quantity = models.IntegerField(default=0)
#     inWayToClient = models.IntegerField(default=0)
#     quantityFull = models.IntegerField(default=0)
#     category = models.CharField(max_length=255, null=True)
#     subject = models.CharField(max_length=255, null=True)
#     brand = models.CharField(max_length=255, null=True)
#     techSize = models.CharField(max_length=255, null=True)
#     isSupply = models.BooleanField(default=False)
#     isRealization = models.BooleanField(default=False)
#     sccode = models.CharField(max_length=255, null=True)

class CeleryLog(models.Model):
    timestamp = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=50)
    message = models.TextField()

    def __str__(self):
        return f"[{self.timestamp}] {self.level}: {self.message}"