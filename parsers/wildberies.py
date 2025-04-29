import asyncio
import random
import httpx
from typing import Union
import time
from celery.utils.log import get_task_logger
import aiohttp
from database.DataBase import async_connect_to_database
from database.funcs_db import get_data_from_db, add_set_data_from_db
from datetime import datetime, timezone, timedelta
from django.utils.dateparse import parse_datetime
import json

logger = get_task_logger("parsers")
moscow_tz = timezone(timedelta(hours=3))

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
    "Content-Type": "application/json; charset=UTF-8",
}


def generate_random_user_agent() -> str:
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
    platforms = [
        "Windows NT 10.0",
        "Windows NT 6.1",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64",
    ]
    versions = [
        lambda: f"{random.randint(70, 110)}.0.{random.randint(0, 9999)}.{random.randint(0, 150)}",
        lambda: f"{random.randint(70, 110)}.0.{random.randint(0, 9999)}",
    ]
    browser = random.choice(browsers)
    platform = random.choice(platforms)
    version = random.choice(versions)()
    return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/{version} Safari/537.36"


def get_data(method: str, url: str, response_type="json", **kwargs):
    attempt, max_attemps = 0, 4
    if headers := kwargs.pop("headers"):
        headers["User-Agent"] = generate_random_user_agent()
    while attempt <= max_attemps:
        time.sleep(5)
        # proxies = {"http://": f"http://{random.choice(proxies_all)}"}
        timeout = httpx.Timeout(10.0, connect=5.0)
        try:
            with httpx.Client(
                timeout=timeout,
                # proxies=proxies
            ) as client:
                response = client.request(
                    method.upper(), url, headers=headers, **kwargs
                )
            if response.status_code == 404:
                logger.info(f"Bad link 404. {url}")
                return None
            if response_type == "json":
                result = response.json()
            elif response_type == "text":
                result = response.text
            return result
        except:
            attempt += 1
            logger.info(f"Can't get data, retry {attempt}")
            time.sleep(attempt * 2)
    logger.error(f"Can't get data, URl: {url}")


def calculate_card_price(price: Union[int, float]) -> int:
    card_price = int((price * 0.97))
    if price >= 15000:
        card_price = 0
    return card_price


def parse_link(
    link: Union[int, str], type
):
    api_url = "https://card.wb.ru/cards/detail"
    params = {
        "spp": "0",
        "reg": "0",
        "appType": "1",
        "emp": "0",
        "dest": -4734876,
        "nm": link,
    }
    data = get_data("get", api_url, "json", headers=headers, params=params)

    if not data or not data["data"]["products"]:
        logger.info(f"Fail {link}. Функция: parse_link. Data: {data}")
        return

    sku = data["data"]["products"][0]

    price = sku.get("priceU", 0) / 100
    promo_price = sku.get("salePriceU", 0) / 100
    if price == promo_price:
        promo_price = 0
    if promo_price > price:
        prices = [price, promo_price]
        price = prices[1]
        promo_price = prices[0]
    if type:
        if isinstance(type, list):
            return [price, promo_price]
        elif type == "price":
            return price
        elif type == "promo_price":
            return promo_price
    card_price = calculate_card_price(promo_price) if promo_price else calculate_card_price(price)
    return card_price

def safe_parse_link(link, type):
    try:
        data = parse_link(link, type)
        return data
    except Exception as e:
        logger.error(f"Can't parse link. Url: {link}. Error: {e}")

def parse_by_links(links: list, type) -> list:
    tasks = [
        safe_parse_link(link, type)
        for link in links
    ]
    return tasks


def parse(links: list, type: Union[str, list] = None) -> list:
    response = parse_by_links(links, type)
    return response

async def wb_api(session, param):
    """
    Асинхронная функция для получения данных по API Wildberries.
    :param param:
    :return:
    """

    API_URL = ''
    view = ''
    data = {}
    params = {}

    if param["type"] == "info_about_rks":
        API_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
        data = param['id_lks']  # максимум 50 рк
        view = "post"

    if param["type"] == "list_adverts_id":
        API_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        view = "get"

    if param["type"] == "get_balance_lk":
        # получить balance-счет net-баланс bonus-бонусы личный кабинет
        # Максимум 1 запрос в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v1/balance"
        view = "get"

    if param["type"] == "orders":
        # Максимум 1 запрос в минуту на один аккаунт продавца
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        params = {
            "dateFrom": param["date_from"], #Дата и время последнего изменения по заказу. `2019-06-20` `2019-06-20T23:59:59`
            "flag": param["flag"],  #если flag=1 то только за выбранный день если 0 то
            # со дня до сегодня но не более 100000 строк
        }
        view = "get"

    if param["type"] == "start_advert":
        # запустить рекламу
        # Максимум 5 запросов в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v0/start"
        params = {
            "id": param["advert_id"],  # int
        }
        view = "get"

    if param["type"] == "budget_advert":
        # получить бюджет кампании
        # Максимум 4 запроса в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v1/budget"
        params = {
            "id": param["advert_id"],  # int
        }
        view = "get"

    if param["type"] == "add_bidget_to_adv":
        # пополнить бюджет рекламной кампании
        # Максимум 1 запрос в секунду на один аккаунт продавца
        API_URL = "https://advert-api.wildberries.ru/adv/v1/budget/deposit"
        params = {
            "id": param["advert_id"],
        }
        data = {
            "sum": param["sum"],  # int
            "type": param["source"],  # int: 0-счет 1-баланс 3-бонусы
            "return": param["return"],  # bool: в ответе вернется обновлённый размер бюджета кампании если True
        }

        view = "post"

    if param["type"] == "get_nmids":
        # получить все артикулы
        # Максимум 100 запросов в минуту для всех методов категории Контент на один аккаунт продавца
        API_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"

        data = {
            "settings": {
                "cursor": {
                    "limit": 100
                },
                "filter": {
                    "withPhoto": -1
                },
            }
        }
        if param.get("updatedAt"):
            data["settings"]["cursor"]["updatedAt"] = param["updatedAt"]
        if param.get("nmID"):
            data["settings"]["cursor"]["nmID"] = param["nmID"]
        view = "post"

    if param["type"] == "get_products_and_prices":
        # получить товары с ценами
        # максимальный лимит 1000
        API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
        params = {
            "limit": param.get("limit", 1000)
        }
        view = "get"

    # сортировка по nmID/предметам/брендам/тегам
    if param["type"] == 'get_stat_cart_sort_nm':
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
        data = {
            "period": {
                "begin": param["begin"],
                "end": param["end"],
            },
            "page": 1
        }
        view = "post"

    if param["type"] == "get_feedback":
        # Максимум 1 запрос в секунду
        # Если превысить лимит в 3 запроса в секунду, отправка запросов будет заблокирована на 60 секунд
        API_URL = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
        params = {
            "isAnswered": param["isAnswered"],  # str: Обработанные отзывы (True) или необработанные отзывы(False)
            "take": param["take"],  # int: Количество отзывов (max. 5 000)
            "skip": param["skip"],  # int: Количество отзывов для пропуска (max. 199990)

        }
        if param.get("nmId"):  # по артикулу
            params["nmId"] = param["nmId"]
        if param.get("order"):  # str: сортировка по дате "dateAsc" "dateDesc"
            params["order"] = param["order"]
        if param.get("dateFrom"):  # int: Дата начала периода в формате Unix timestamp
            params["dateFrom"] = param["dateFrom"]
        if param.get("dateTo"):  # int: Дата конца периода в формате Unix timestamp
            params["dateTo"] = param["dateTo"]
        view = "get"

    if param["type"] == "warehouse_data":
        # Максимум 3 запроса в минуту на один аккаунт продавца
        # Метод формирует набор данных об остатках по складам.
        # Данные по складам Маркетплейс (FBS) приходят в агрегированном виде — по всем сразу, без детализации по
        # конкретным складам — эти записи будут с "regionName":"Маркетплейс" и "offices":[].
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/offices"

        data = {
            "currentPeriod": {
                "start": param["start"], #"2024-02-10" Не позднее end. Не ранее 3 месяцев от текущей даты
                "end": param["end"], #Дата окончания периода. Не ранее 3 месяцев от текущей даты
            },
            "stockType": "" if not param.get("stockType") else param["stockType"], #"" — все wb—Склады WB mp—Склады Маркетплейс (FBS)
            "skipDeletedNm": True if not param.get("skipDeletedNm") else param["skipDeletedNm"], #Скрыть удалённые товары
        }

        view = 'post'

    if param["type"] == "get_stocks_data":
        # Метод предоставляет количество остатков товаров на складах WB.
        # Данные обновляются раз в 30 минут.
        # Максимум 1 запрос в минуту на один аккаунт продавца

        params = {"dateFrom": param["dateFrom"]} #"2019-06-20"  Время передаётся в часовом поясе Мск (UTC+3).
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

        view = "get"


    headers = {
        "Authorization": f"Bearer {param['API_KEY']}"  # Или просто API_KEY, если нужно
    }

    if view == 'get':
        try:
            async with session.get(API_URL, headers=headers, params=params, timeout=60, ssl=False) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка в wb_api (get запрос): {e}. Параметры: {param}")
            return response

    if view == 'post':
        try:
            async with session.post(API_URL, headers=headers, params=params, json=data, timeout=60,
                                    ssl=False) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка в wb_api (post запрос): {e}. Параметры: {param}")
            return response


async def get_products_and_prices():
    """
    получаем товары и цены и пишем их в бд
    :return:
    """

    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})

    data = {}

    async with aiohttp.ClientSession() as session:
        for cab in cabinets:
            param = {
                "type": "get_products_and_prices",
                "API_KEY": cab["token"],
            }

            data[cab["id"]] = wb_api(session, param)

        results = await asyncio.gather(*data.values())
        id_to_result = {name: result for name, result in zip(data.keys(), results)}

        try:
            conn = await async_connect_to_database()
            if not conn:
                logger.warning("Ошибка подключения к БД")
                raise
            for key, value in id_to_result.items():
                value = value["data"]["listGoods"]
                data = []
                try:
                    for item in value:
                        data.append(
                            add_set_data_from_db(
                                conn=conn,
                                table_name="myapp_price",
                                data=dict(
                                    lk_id=key,
                                    nmid=item["nmID"],
                                    vendorcode=item["vendorCode"],
                                    sizes=json.dumps(item["sizes"]),
                                    discount=item["discount"],
                                    clubdiscount=item["clubDiscount"],
                                    editablesizeprice=item["editableSizePrice"],
                                ),
                                conflict_fields=["nmid", "lk_id"]
                            )
                        )
                    results = await asyncio.gather(*data)
                except Exception as e:
                    logger.error(f"Ошибка при добавлении продуктов и цен {e}")
        except:
            return
        finally:
            await conn.close()


async def get_nmids():
    # получаем все карточки товаров
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})

    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_nmids",
                "API_KEY": cab["token"],
            }
            while True:
                response = await wb_api(session, param)
                if not response.get("cards"):
                    logger.error(f"Ошибка при получении артикулов: {response}")
                    raise
                conn = await async_connect_to_database()
                if not conn:
                    logger.warning("Ошибка подключения к БД")
                    raise
                try:
                    for resp in response["cards"]:
                        await add_set_data_from_db(
                            conn=conn,
                            table_name="myapp_nmids",
                            data=dict(
                                lk_id=cab["id"],
                                nmid=resp["nmID"],
                                imtid=resp["imtID"],
                                nmuuid=resp["nmUUID"],
                                subjectid=resp["subjectID"],
                                subjectname=resp["subjectName"],
                                vendorcode=resp["vendorCode"],
                                brand=resp["brand"],
                                title=resp["title"],
                                description=resp["description"],
                                needkiz=resp["needKiz"],
                                dimensions=json.dumps(resp["dimensions"]),
                                characteristics=json.dumps(resp["characteristics"]),
                                sizes=json.dumps(resp["sizes"]),
                                created_at=parse_datetime(resp["createdAt"]),
                                updated_at=parse_datetime(resp["updatedAt"]),
                                added_db=datetime.now(moscow_tz)
                            ),
                            conflict_fields=["nmid", "lk_id"]
                        )
                except Exception as e:
                    logger.error(f"Ошибка при добавлении артикулов в бд {e}")
                    raise
                finally:
                    conn.close()


                if response["cursor"]["total"] < 100:
                    break
                else:
                    param["updatedAt"] = response["cursor"]["updatedAt"]
                    param["nmID"] = response["cursor"]["nmID"]

# async def get_stocks_data():
#     # cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})
#
#     async with aiohttp.ClientSession() as session:
#         param = {
#             "type": "get_stocks_data",
#             "API_KEY": "",
#             "dateFrom": "2025-04-28T17:16:00", #вчерашний день с текущим временем
#         }
#         quantity = 0
#         response = await wb_api(session, param)
#         for i in response:
#             if i["nmId"] == 219934666:
#                 quantity += i["quantity"]
#         a = res
#
#         param = {
#             "type": "orders",
#             "API_KEY": "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQxMTE4djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc0OTE1NjE0NCwiaWQiOiIwMTkzOTVmYy0wZGFhLTdiOGUtYTk5MC0zMDc3ZjIwNzliZWQiLCJpaWQiOjE0ODU3Mzg5Nywib2lkIjo0MDY3NjgwLCJzIjozODM4LCJzaWQiOiI2Y2UwYjFiMy1jOGU0LTRjYzYtYThjYS01MmRmNTQ4ZTk5MjUiLCJ0IjpmYWxzZSwidWlkIjoxNDg1NzM4OTd9.DrVmBZGRyBGwpCaCrspxkX1aokpo09gmLmj1IUIiqR4MutSLxzPU5gxjeKvdktLzzDkptodtvvSBm7Ga4j5ZHw",
#             "date_from": "2025-04-15T00:00:00",
#             "flag": 0
#         }
#         order = 0
#         response = await wb_api(session, param)
#         for i in response:
#             if i["nmId"] == 219934666 and datetime.fromisoformat("2025-04-29T00:00:00") >= datetime.fromisoformat(i["date"]) >= datetime.fromisoformat("2025-04-15T00:00:00"):
#                 order +=1
#         a = order
#
#
# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(get_nmids())