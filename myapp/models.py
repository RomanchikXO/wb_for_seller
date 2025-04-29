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


class Stocks(models.Model):
    lk = models.ForeignKey(WbLk, on_delete=models.CASCADE, default=1)  # lk_id в бд
    lastchangedate = models.DateTimeField() #Дата и время обновления информации в сервисе. Это поле соответствует параметру dateFrom в запросе. Если часовой пояс не указан, то берётся Московское время (UTC+3)
    warehousename = models.CharField(max_length=255) #Название склада
    supplierarticle = models.CharField(max_length=255) #Артикул продавца
    nmid = models.IntegerField() #Артикул
    barcode = models.IntegerField() #Баркод
    quantity = models.IntegerField() #Количество, доступное для продажи (сколько можно добавить в корзину)
    inwaytoclient = models.IntegerField() #В пути к клиенту
    inwayfromclient = models.IntegerField() #В пути от клиента
    quantityfull = models.IntegerField(default=0) #Полное (непроданное) количество, которое числится за складом (= quantity + в пути)
    category = models.CharField(max_length=255, null=True) #Категория
    techsize = models.CharField(max_length=255, null=True) #Размер
    issupply = models.BooleanField(default=False) #Договор поставки (внутренние технологические данные)
    isrealization = models.BooleanField(default=False) #Договор реализации (внутренние технологические данные)
    sccode = models.CharField(max_length=255, null=True) #Код контракта (внутренние технологические данные)

    class Meta:
        unique_together = ['nmid', 'lk', 'supplierarticle']
        verbose_name_plural = "Отстаки товаров на складах"

    def __str__(self):
        return f"{self.supplierarticle} | {self.techsize} | {self.quantity} шт."

class CeleryLog(models.Model):
    timestamp = models.DateTimeField(auto_now_add=True)
    level = models.CharField(max_length=50)
    message = models.TextField()

    def __str__(self):
        return f"[{self.timestamp}] {self.level}: {self.message}"