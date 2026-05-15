import asyncio
import random
import httpx
from typing import Union, List
import time
import aiohttp
from database.DataBase import async_connect_to_database
from database.funcs_db import get_data_from_db, add_set_data_from_db
from datetime import datetime, timedelta, date
from django.utils.dateparse import parse_datetime
import json
import uuid
import zipfile
import math
import logging
import io
import csv
import re
from context_logger import ContextLogger
from itertools import chain
from myapp.models import Price
from asgiref.sync import sync_to_async
from parsers.utils import _to_int, _to_float


logger = ContextLogger(logging.getLogger("parsers"))

STORY_STOCK_FIRST_RUN_REQUESTS = 46
STORY_STOCK_REQUEST_DELAY_SECONDS = 62
STOCK_BY_DAY_REQUEST_DELAY_SECONDS = 22


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
    "Content-Type": "application/json; charset=UTF-8",
}


def get_uuid()-> str:
    generated_uuid = str(uuid.uuid4())
    return generated_uuid


def normalize_warehouse_name(name: str) -> str:
    if not name:
        return ""

    normalized = name.lower().replace("ё", "е")
    normalized = re.sub(r"\bсц\b", " ", normalized)
    normalized = normalized.replace("кгт+", " ")
    normalized = re.sub(r"\bсгт\b|\bкгт\b", " ", normalized)
    normalized = re.sub(r"[()\-/,]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def build_unique_normalized_map(names_to_ids: dict[str, int]) -> dict[str, int]:
    normalized_candidates: dict[str, set[int]] = {}
    for name, object_id in names_to_ids.items():
        normalized_name = normalize_warehouse_name(name)
        normalized_candidates.setdefault(normalized_name, set()).add(object_id)

    return {
        normalized_name: next(iter(object_ids))
        for normalized_name, object_ids in normalized_candidates.items()
        if normalized_name and len(object_ids) == 1
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
    link: Union[int, str], disc: int
) -> tuple:
    api_url = "https://card.wb.ru/cards/v4/detail"
    params = {
        "spp": "0",
        "reg": "0",
        "appType": "1",
        "emp": "0",
        "dest": -4734876,
        "nm": link,
    }
    data = get_data("get", api_url, "json", headers=headers, params=params)

    if not data or not data["products"][0]:
        logger.info(f"Fail {link}. Функция: parse_link. Data: {data}")
        return 0, 0

    sku = data["products"][0]

    try:
        price = int(int(sku["sizes"][0]["price"]["product"] / 100 * 0.97) * ((100-disc) / 100))
    except Exception as e:
        logger.info(f"Не нашли цену для артикула {link}. Ошибка {e}")
        price = 0

    rating = sku.get("reviewRating", 0)

    return price, rating


def safe_parse_link(link, disc: int) -> tuple:
    try:
        data = parse_link(link, disc)
        return data
    except Exception as e:
        logger.error(f"Can't parse link. Url: {link}. Error: {e}")

def parse_by_links(links: list, disc: int) -> List[tuple]:
    tasks = [
        safe_parse_link(link, disc)
        for link in links
    ]
    return tasks


def parse(links: list, disc: int) -> List[tuple]:
    response = parse_by_links(links, disc)
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
        # Данные обновляются раз в 30 минут.
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

    if param["type"] == "get_delivery_fbw":
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/incomes"

        params = {
            "dateFrom": param["dateFrom"]
        }

        view = "get"

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

    if param["type"] == "get_story_stock_by_day":
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/products/sizes"
        data = {
            "nmID": param["nmID"],
            "currentPeriod": {
                "start": param["start"],
                "end": param["end"],
            },
            "stockType": param.get("stockType", ""),
            "skipDeletedNm": param.get("skipDeletedNm", True), # Скрыть удалённые товары
            "orderBy": {
                "field": param.get("orderBy", "stockCount"),
                "mode": param.get("mode", "desc"), # asc - по возрастанию, desc - по убыванию
            },
            "includeOffice": param.get("includeOffice", True), # bool: включить склады
        }
        view = "post"

    if param["type"] == "get_story_stock":
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/products/products"
        story_stock_statuses = [
            "deficient",
            "actual",
            "balanced",
            "nonActual",
            "nonLiquid",
            "invalidData"
        ]
        data = {
            "currentPeriod": {
                "start": param["start"],
                "end": param["end"],
            },
            "stockType": param.get("stockType", ""),
            "skipDeletedNm": param.get("skipDeletedNm", True), # Скрыть удалённые товары
            "orderBy": {
                "field": param.get("orderBy", "stockCount"),
                "mode": param.get("mode", "desc"), # asc - по возрастанию, desc - по убыванию
            },
            "availabilityFilters": param.get("availabilityFilters", story_stock_statuses), # list[str]
            "limit": param.get("limit", 1000), # int: Максимальное количество товаров
            "offset": param.get("offset", 0), # int: Смещение
        }
        view = "post"

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

        view = "post"

    if param["type"] == "seller_analytics_generate":
        # Метод создаёт задание на генерацию отчёта с расширенной аналитикой продавца.
        # Максимальное количество отчётов, генерируемых в сутки — 20.
        # Максимум 3 запроса в минуту на один аккаунт продавца
        API_URL = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads"

        # https://dev.wildberries.ru/openapi/analytics#tag/Analitika-prodavca-CSV/paths/~1api~1v2~1nm-report~1downloads/post
        # Ниже типы reportType
        # DETAIL_HISTORY_REPORT GROUPED_HISTORY_REPORT SEARCH_QUERIES_PREMIUM_REPORT_GROUP
        # SEARCH_QUERIES_PREMIUM_REPORT_PRODUCT SEARCH_QUERIES_PREMIUM_REPORT_TEXT STOCK_HISTORY_REPORT_CSV

        statuses = [
            "deficient",
            "actual",
            "balanced",
            "nonActual",
            "nonLiquid",
            "invalidData"
        ]

        data = {
            "id": param["id"], # ID отчёта в UUID-формате
            "reportType": param["reportType"],
            "userReportName": param["userReportName"], # Название отчета
        }
        if param["reportType"] == "DETAIL_HISTORY_REPORT":
            data["params"] = {
                "startDate": param["start"],  # str
                "endDate": param["end"],
                "skipDeletedNm": param.get("skipDeletedNm", True),  # скрыть удаленные товары
            }
        elif param["reportType"] == "STOCK_HISTORY_REPORT_CSV":
            data["params"] = {
                "currentPeriod": {
                    "start": param["start"],
                    "end": param["end"],
                },  # str
                "stockType": param.get("stockType", ""),
                "skipDeletedNm": param.get("skipDeletedNm", True),  # скрыть удаленные товары
                "availabilityFilters": param.get("availabilityFilters", statuses), # List[str]
                "orderBy": {
                    "field": param.get("orderBy", "officeMissingTime"),
                    "mode": param.get("mode", "desc"),
                }
            }

        view = "post"

    if param["type"] == "seller_analytics_report":
        # Можно получить отчёт, который сгенерирован за последние 48 часов.
        # Отчёт будет загружен внутри архива ZIP в формате CSV.
        # Максимум 3 запроса в минуту на один аккаунт продавца
        API_URL = f"https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads/file/{param['downloadId']}"

        params = {
            "downloadId": param["downloadId"], # string <uuid>
        }
        view = "get"

    if param["type"] == "get_stocks_data":
        # Метод предоставляет количество остатков товаров на складах WB.
        # Данные обновляются раз в 30 минут.
        # Максимум 1 запрос в минуту на один аккаунт продавца

        params = {"dateFrom": param["dateFrom"]} #"2019-06-20"  Время передаётся в часовом поясе Мск (UTC+3).
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

        view = "get"

    if param["type"] == "get_warhouse":
        # метод для получения складов
        API_URL = "https://marketplace-api.wildberries.ru/api/v3/offices"
        view = "get"

    if param["type"] == "set_price_and_discount":
        # Метод устанавливает цены и скидки для товаров.
        # Максимум 10 запросов за 6 секунд
        # Максимум 1 000 товаров
        # Цена и скидка не могут быть пустыми одновременно
        # Если новая цена со скидкой будет хотя бы в 3 раза меньше старой, она попадёт в карантин, и товар будет продаваться по старой цене
        API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"

        data = {
            "data": param["data"]
        } # List[dict]  где dict {"nmID": int, "price": int, "discount": int}
        view = "post"

    if param["type"] == "get_question":
        # Метод предоставляет список вопросов по заданным фильтрам.
        # Можно получить максимум 10 000 вопросов в одном ответе
        # Максимум 1 запрос в секунду
        # Если превысить лимит в 3 запроса в секунду, отправка запросов будет заблокирована на 60 секунд

        API_URL = "https://feedbacks-api.wildberries.ru/api/v1/questions"
        params = {
            "isAnswered": param["isAnswered"], # bool отвеченные (True)
            "take": param.get("take", 10000), # Количество запрашиваемых вопросов (максимально допустимое значение для параметра - 10 000, при этом сумма значений параметров take и skip не должна превышать 10 000)
            "skip": param.get("skip", 0), # Количество вопросов для пропуска (максимально допустимое значение для параметра - 10 000, при этом сумма значений параметров take и skip не должна превышать 10 000)
        }
        view = "get"


    headers = {
        "Authorization": f"Bearer {param['API_KEY']}"  # Или просто API_KEY, если нужно
    }

    if view == 'get':
        async with session.get(API_URL, headers=headers, params=params, timeout=60, ssl=False) as response:
            if param["type"] == "seller_analytics_report":
                try:
                    content = await response.read()
                    return content
                except Exception as e:
                    return e
            response_text = await response.text()
            try:
                response.raise_for_status()
                return json.loads(response_text)
            except Exception as e:
                logger.error(
                    f"Ошибка в wb_api (get запрос): {e}. Ответ: {response_text}. Параметры: {param}"
                )
                return None

    if view == 'post':
        async with session.post(API_URL, headers=headers, params=params, json=data, timeout=60,
                                ssl=False) as response:
            response_text = await response.text()
            try:
                response.raise_for_status()
                return json.loads(response_text)
            except Exception as e:
                logger.error(
                    f"Ошибка в wb_api (post запрос): {e}.  Ответ: {response_text}. Параметры: {param}"
                )
                return None


async def get_products_and_prices():
    """
    получаем товары и цены и пишем их в бд
    :return:
    """

    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})

    data = {}

    status_rep = await sync_to_async(
        lambda: Price.objects.order_by('id').values_list('main_status', flat=True).first()
    )()

    async with aiohttp.ClientSession() as session:
        for cab in cabinets:
            param = {
                "type": "get_products_and_prices",
                "API_KEY": cab["token"],
            }

            data[cab["id"]] = wb_api(session, param)

        try:
            results = await asyncio.gather(*data.values())
            id_to_result = {name: result for name, result in zip(data.keys(), results)}
        except Exception as e:
            logger.error(f"Ошибка при обработке данных полученных от вб в get_products_and_prices {e}")
            return

        try:
            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
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
                                    main_status=status_rep,
                                ),
                                conflict_fields=["nmid", "lk_id"]
                            )
                        )
                    results = await asyncio.gather(*data)
                except Exception as e:
                    logger.error(f"Ошибка при добавлении продуктов и цен {e}")
        except Exception as e:
            logger.error(f"Глобальная ошибка при добавлении продуктов и цен: {e}")
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
                    logger.error("Ошибка подключения к БД")
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
                                description=resp.get("description"),
                                needkiz=resp["needKiz"],
                                dimensions=json.dumps(resp["dimensions"]),
                                characteristics=json.dumps(resp["characteristics"]),
                                sizes=json.dumps(resp["sizes"]),
                                tag_ids = json.dumps([]),
                                created_at=parse_datetime(resp["createdAt"]),
                                updated_at=parse_datetime(resp["updatedAt"]),
                                added_db=datetime.now() + timedelta(hours=3)
                            ),
                            conflict_fields=["nmid", "lk_id"]
                        )
                except Exception as e:
                    logger.error(f"Ошибка при добавлении артикулов в бд {e}")
                    raise
                finally:
                    await conn.close()


                if response["cursor"]["total"] < 100:
                    break
                else:
                    param["updatedAt"] = response["cursor"]["updatedAt"]
                    param["nmID"] = response["cursor"]["nmID"]


async def get_warhouse():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})
    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_warhouse",
                "API_KEY": cab["token"],
            }
            response = await wb_api(session, param)

            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
                raise
            try:
                for resp in response:
                    await add_set_data_from_db(
                        conn=conn,
                        table_name="myapp_warhouses",
                        data=dict(
                            name=resp["name"],
                            address=resp["address"],
                            city=resp["city"],
                            id=resp["id"],
                            longitude=resp["longitude"],
                            latitude=resp["latitude"],
                            cargoType=resp["cargoType"],
                            deliveryType=resp["deliveryType"],
                            federalDistrict=resp["federalDistrict"],
                            selected=resp["selected"],
                        )
                    )
            except Exception as e:
                logger.error(f"Ошибка при добавлении складов в БД. Error: {e}")
            finally:
                await conn.close()


async def get_stocks_data():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})

    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_stocks_data",
                "API_KEY": cab["token"],
                "dateFrom": str(datetime.now() + timedelta(hours=3) - timedelta(days=250)),
            }
            response = await wb_api(session, param)

            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
                raise
            try:
                warhouses = await conn.fetch('SELECT id, name FROM "myapp_warhouses"')
                warehouse_ids_by_name = {
                    row["name"]: row["id"]
                    for row in warhouses
                }
                warehouse_ids_by_normalized_name = build_unique_normalized_map(warehouse_ids_by_name)

                alias_rows = await conn.fetch(
                    '''
                    SELECT source_name, normalized_name, warehouse_id
                    FROM "myapp_warehousealias"
                    WHERE source_type = $1 AND is_active = TRUE
                    ''',
                    "stocks",
                )
                alias_ids_by_name = {
                    row["source_name"]: row["warehouse_id"]
                    for row in alias_rows
                    if row["warehouse_id"] is not None
                }
                alias_normalized_candidates = {}
                for row in alias_rows:
                    if row["warehouse_id"] is None or not row["normalized_name"]:
                        continue
                    alias_normalized_candidates.setdefault(row["normalized_name"], set()).add(row["warehouse_id"])
                alias_ids_by_normalized_name = {
                    normalized_name: next(iter(object_ids))
                    for normalized_name, object_ids in alias_normalized_candidates.items()
                    if len(object_ids) == 1
                }

                # Ключи актуальных остатков из текущего ответа WB для конкретного кабинета.
                current_keys = set()
                unknown_aliases = {}

                for quant in response:
                    barcode = int(quant["barcode"]) if quant.get("barcode") else None
                    if barcode is None:
                        continue

                    warehousename = quant["warehouseName"]
                    normalized_warehousename = normalize_warehouse_name(warehousename)
                    warhouse_id = (
                        alias_ids_by_name.get(warehousename)
                        or alias_ids_by_normalized_name.get(normalized_warehousename)
                        or warehouse_ids_by_name.get(warehousename)
                        or warehouse_ids_by_normalized_name.get(normalized_warehousename)
                    )
                    if warhouse_id is None and warehousename not in unknown_aliases:
                        unknown_aliases[warehousename] = normalized_warehousename

                    stock_key = (
                        quant["nmId"],
                        barcode,
                        warehousename,
                    )
                    current_keys.add(stock_key)

                    await add_set_data_from_db(
                        conn=conn,
                        table_name="myapp_stocks",
                        data=dict(
                            lk_id=cab["id"],
                            lastchangedate=parse_datetime(quant["lastChangeDate"]),
                            warehousename=warehousename,
                            warhouse_id_id=warhouse_id,
                            supplierarticle=quant["supplierArticle"],
                            nmid=quant["nmId"],
                            barcode=barcode,
                            quantity=quant["quantity"],
                            inwaytoclient=quant["inWayToClient"],
                            inwayfromclient=quant["inWayFromClient"],
                            quantityfull=quant["quantityFull"],
                            category=quant["category"],
                            techsize=quant["techSize"],
                            issupply=quant["isSupply"],
                            isrealization=quant["isRealization"],
                            sccode=quant["SCCode"],
                            added_db=datetime.now() + timedelta(hours=3)

                        ),
                        conflict_fields=['nmid', 'lk_id', 'barcode', 'warehousename']
                    )

                existing_rows = await conn.fetch(
                    """
                    SELECT nmid, barcode, warehousename
                    FROM myapp_stocks
                    WHERE lk_id = $1
                    """,
                    cab["id"],
                )

                existing_keys = {
                    (row["nmid"], row["barcode"], row["warehousename"])
                    for row in existing_rows
                }
                stale_keys = existing_keys - current_keys

                if stale_keys:
                    delete_query = """
                        DELETE FROM myapp_stocks
                        WHERE lk_id = $1
                          AND nmid = $2
                          AND barcode IS NOT DISTINCT FROM $3::bigint
                          AND warehousename IS NOT DISTINCT FROM $4::text
                    """
                    await conn.executemany(
                        delete_query,
                        [
                            (cab["id"], nmid, barcode, warehousename)
                            for nmid, barcode, warehousename in stale_keys
                        ],
                    )

                if unknown_aliases:
                    await conn.executemany(
                        '''
                        INSERT INTO "myapp_warehousealias" ("source_name", "normalized_name", "source_type", "is_active")
                        VALUES ($1, $2, $3, TRUE)
                        ON CONFLICT ("source_name", "source_type")
                        DO UPDATE SET "normalized_name" = EXCLUDED."normalized_name"
                        ''',
                        [
                            (source_name, normalized_name, "stocks")
                            for source_name, normalized_name in unknown_aliases.items()
                        ],
                    )
                    logger.warning(
                        f"Не удалось автоматически сопоставить {len(unknown_aliases)} складов из отчета остатков. "
                        f"Добавил их в алиасы складов для ручной настройки."
                    )
            except Exception as e:
                logger.error(f"Ошибка при добавлении остатков в БД. Error: {e}")
            finally:
                await conn.close()


async def get_story_stock():
    """
    Сохраняет срез по товарам из API stocks-report/products/products.
    Первый запуск для кабинета: 46 запросов (сегодня и 45 дней назад) с паузой 62 сек.
    Последующие запуски: 1 запрос за вчерашнюю дату.
    """
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})
    if not cabinets:
        logger.warning("Не найдены кабинеты для get_story_stock")
        return

    today = date.today()

    for cab in cabinets:
        conn = await async_connect_to_database()
        if not conn:
            logger.error(f"Ошибка подключения к БД в get_story_stock. Кабинет {cab['name']}")
            continue

        try:
            story_stock_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM myapp_storystock
                WHERE lk_id = $1
                """,
                cab["id"]
            )

            is_first_run = (story_stock_count or 0) == 0
            request_dates = (
                [today - timedelta(days=i) for i in range(STORY_STOCK_FIRST_RUN_REQUESTS)]
                if is_first_run else
                [today - timedelta(days=1)]
            )

            logger.info(
                f"get_story_stock: кабинет {cab['name']} ({cab['id']}), "
                f"{'первый запуск' if is_first_run else 'обычный запуск'}, "
                f"запросов: {len(request_dates)}"
            )

            async with aiohttp.ClientSession() as session:
                for index, current_day in enumerate(request_dates):
                    period_day = current_day.strftime('%Y-%m-%d')
                    param = {
                        "type": "get_story_stock",
                        "API_KEY": cab["token"],
                        "start": period_day,
                        "end": period_day,
                        "skipDeletedNm": True,
                        "orderBy": "stockCount",
                        "mode": "desc",
                        "limit": 1000,
                        "offset": 0,
                    }
                    response = await wb_api(session, param)
                    items = (((response or {}).get("data") or {}).get("items")) or []

                    if response is None:
                        logger.error(
                            f"Пустой ответ get_story_stock. Кабинет {cab['name']}. "
                            f"Период: {period_day}"
                        )
                    else:
                        for item in items:
                            nmid = item.get("nmID")
                            if nmid is None:
                                continue

                            metrics = item.get("metrics") or {}
                            await add_set_data_from_db(
                                conn=conn,
                                table_name="myapp_storystock",
                                data=dict(
                                    lk_id=cab["id"],
                                    date_wb=current_day,
                                    nmid=_to_int(nmid),
                                    subjectname=item.get("subjectName"),
                                    name=item.get("name"),
                                    vendorcode=item.get("vendorCode"),
                                    brandname=item.get("brandName"),
                                    orderscount=_to_int(metrics.get("ordersCount")),
                                    orderssum=_to_float(metrics.get("ordersSum")),
                                    buyoutcount=_to_int(metrics.get("buyoutCount")),
                                    buyoutsum=_to_float(metrics.get("buyoutSum")),
                                    stockcount=_to_int(metrics.get("stockCount")),
                                ),
                                conflict_fields=["lk_id", "date_wb", "nmid"]
                            )

                        logger.info(
                            f"get_story_stock: кабинет {cab['name']}, период {period_day}, "
                            f"сохранено записей: {len(items)}"
                        )

                    if index < len(request_dates) - 1:
                        await asyncio.sleep(STORY_STOCK_REQUEST_DELAY_SECONDS)

        except Exception as e:
            logger.exception(f"Ошибка в get_story_stock для кабинета {cab['name']}: {e}")
        finally:
            await conn.close()


def _extract_office_stock(metrics: dict) -> int:
    return _to_int((metrics or {}).get("stockCount"))


def _aggregate_stock_by_warehouse(response_data: dict) -> dict[str, int]:
    """
    Собирает остатки по складам из ответа stocks-report/products/sizes.
    Приоритет: sizes[].offices (детальнее), fallback: data.offices.
    """
    warehouse_stock: dict[str, int] = {}
    sizes = (response_data or {}).get("sizes") or []

    if sizes:
        for size_item in sizes:
            for office in (size_item or {}).get("offices") or []:
                warehouse_name = (office or {}).get("officeName")
                if not warehouse_name:
                    continue
                stock = _extract_office_stock((office or {}).get("metrics") or {})
                if stock > 0:
                    warehouse_stock[warehouse_name] = warehouse_stock.get(warehouse_name, 0) + stock
        return warehouse_stock

    for office in (response_data or {}).get("offices") or []:
        warehouse_name = (office or {}).get("officeName")
        if not warehouse_name:
            continue
        stock = _extract_office_stock((office or {}).get("metrics") or {})
        if stock > 0:
            warehouse_stock[warehouse_name] = warehouse_stock.get(warehouse_name, 0) + stock
    return warehouse_stock


async def _load_active_nmids_for_cabinet(cabinet_id: int) -> dict[int, dict]:
    rows = await get_data_from_db(
        "myapp_nmids",
        ["nmid", "vendorcode", "title", "subjectname", "brand"],
        conditions={"lk_id": cabinet_id},
        additional_conditions="is_active = TRUE",
    )
    if not rows:
        return {}

    data = {}
    for row in rows:
        nmid_value = row.get("nmid")
        if nmid_value is None:
            continue
        data[_to_int(nmid_value)] = {
            "vendorcode": row.get("vendorcode") or str(nmid_value),
            "name": row.get("title"),
            "subject": row.get("subjectname"),
            "brand": row.get("brand"),
        }
    return data


async def _process_stock_by_day_for_cabinet(cab: dict, request_dates: list[date]) -> None:
    active_products = await _load_active_nmids_for_cabinet(cab["id"])
    if not active_products:
        logger.info(
            f"get_story_stock_by_day: кабинет {cab['name']} ({cab['id']}) "
            f"- нет активных nmids (is_active=True)"
        )
        return

    conn = await async_connect_to_database()
    if not conn:
        logger.error(f"Ошибка подключения к БД в get_story_stock_by_day. Кабинет {cab['name']}")
        return

    try:
        async with aiohttp.ClientSession() as session:
            total_requests = len(active_products) * len(request_dates)
            request_index = 0

            for nmid, meta in active_products.items():
                for target_day in request_dates:
                    request_index += 1
                    period_day = target_day.strftime("%Y-%m-%d")
                    param = {
                        "type": "get_story_stock_by_day",
                        "API_KEY": cab["token"],
                        "nmID": nmid,
                        "start": period_day,
                        "end": period_day,
                        "skipDeletedNm": True,
                        "orderBy": "stockCount",
                        "mode": "desc",
                        "includeOffice": True,
                    }

                    response = await wb_api(session, param)
                    response_data = (response or {}).get("data") or {}
                    warehouse_stock = _aggregate_stock_by_warehouse(response_data)

                    if response is None:
                        logger.error(
                            f"Пустой ответ get_story_stock_by_day. Кабинет {cab['name']}, "
                            f"nmid={nmid}, период={period_day}"
                        )
                    elif not warehouse_stock:
                        logger.info(
                            f"get_story_stock_by_day: кабинет {cab['name']}, nmid={nmid}, "
                            f"период={period_day} - остатков > 0 нет"
                        )
                    else:
                        for warehouse_name, stock_value in warehouse_stock.items():
                            await conn.execute(
                                """
                                INSERT INTO myapp_stockbyday (
                                    lk_id, vendorcode, name, nmid, subject, brand,
                                    size, warehouse, date_wb, stock
                                )
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                ON CONFLICT (lk_id, vendorcode, nmid, warehouse, date_wb) DO UPDATE SET
                                    name = EXCLUDED.name,
                                    subject = EXCLUDED.subject,
                                    brand = EXCLUDED.brand,
                                    size = EXCLUDED.size,
                                    stock = EXCLUDED.stock
                                """,
                                cab["id"],
                                meta["vendorcode"],
                                meta["name"],
                                nmid,
                                meta["subject"],
                                meta["brand"],
                                "",
                                warehouse_name,
                                target_day,
                                stock_value,
                            )

                    if request_index < total_requests:
                        await asyncio.sleep(STOCK_BY_DAY_REQUEST_DELAY_SECONDS)
    except Exception as e:
        logger.exception(
            f"Ошибка в get_story_stock_by_day для кабинета {cab['name']} ({cab['id']}): {e}"
        )
    finally:
        await conn.close()


async def get_story_stock_by_day():
    """
    Для каждого кабинета собирает активные nmids (is_active=True) и запрашивает
    остатки по складам только за вчерашний день. Ограничение API соблюдается:
    максимум 1 запрос в 22 секунды на один кабинет.
    """
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={"groups_id": 1})
    if not cabinets:
        logger.warning("Не найдены кабинеты для get_story_stock_by_day")
        return

    msk_today = (datetime.now() + timedelta(hours=3)).date()
    request_dates = [msk_today - timedelta(days=1)]

    await asyncio.gather(*[
        _process_stock_by_day_for_cabinet(cab, request_dates)
        for cab in cabinets
    ])


async def get_orders():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})
    for cab in cabinets:
        async with aiohttp.ClientSession() as session:
            date_from = (datetime.now() + timedelta(hours=3) - timedelta(days=30)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            param = {
                "type": "orders",
                "API_KEY": cab["token"],
                "date_from": date_from.strftime("%Y-%m-%dT%H:%M:%S"),
                "flag": 0
            }
            response = await wb_api(session, param)

            if not isinstance(response, list):
                logger.error(
                    f"Некорректный ответ WB в get_orders. Кабинет: {cab['name']}. "
                    f"dateFrom={param['date_from']}. Ответ: {response}"
                )
                continue
            if not response:
                logger.info(
                    f"В get_orders нет новых заказов. Кабинет: {cab['name']}. "
                    f"dateFrom={param['date_from']}"
                )
                continue

            conn = await async_connect_to_database()
            if not conn:
                logger.warning("Ошибка подключения к БД")
                raise
            saved_count = 0
            skipped_count = 0
            try:
                for order in response:
                    try:
                        cancel_date = parse_datetime(order.get("cancelDate")) or datetime(1970, 1, 1)
                        await add_set_data_from_db(
                            conn=conn,
                            table_name="myapp_orders",
                            data=dict(
                                lk_id=cab["id"],
                                date=parse_datetime(order.get("date")) or datetime(1970, 1, 1),
                                lastchangedate=parse_datetime(order.get("lastChangeDate")) or datetime(1970, 1, 1),
                                warehousename=(
                                    order.get("warehouseName", "").replace("Виртуальный ", "")
                                    if order.get("warehouseName", "").startswith("Виртуальный")
                                    else order.get("warehouseName", "")
                                ),
                                warehousetype=order.get("warehouseType", ""),
                                countryname=order.get("countryName", ""),
                                oblastokrugname=order.get("oblastOkrugName"),
                                regionname=order.get("regionName"),
                                supplierarticle=order.get("supplierArticle", ""),
                                nmid=_to_int(order.get("nmId")) or 0,
                                barcode=_to_int(order.get("barcode")),
                                category=order.get("category", ""),
                                subject=order.get("subject", ""),
                                brand=order.get("brand", ""),
                                techsize=order.get("techSize"),
                                incomeid=_to_int(order.get("incomeID")) or 0,
                                issupply=bool(order.get("isSupply", False)),
                                isrealization=bool(order.get("isRealization", False)),
                                totalprice=_to_int(order.get("totalPrice")) or 0,
                                discountpercent=_to_int(order.get("discountPercent")) or 0,
                                spp=_to_int(order.get("spp")) or 0,
                                finishedprice=_to_float(order.get("finishedPrice")) or 0.0,
                                pricewithdisc=_to_float(order.get("priceWithDisc")) or 0.0,
                                iscancel=bool(order.get("isCancel", False)),
                                canceldate=cancel_date,
                                sticker=order.get("sticker", ""),
                                gnumber=order.get("gNumber", ""),
                                srid=order.get("srid", ""),
                            ),
                            conflict_fields=['nmid', 'lk_id', 'srid']
                        )
                        saved_count += 1
                    except Exception as order_error:
                        skipped_count += 1
                        logger.error(
                            f"Пропуск заказа в get_orders. Кабинет: {cab['name']}. "
                            f"srid={order.get('srid')}. Error: {order_error}"
                        )
            except Exception as e:
                logger.error(f"Ошибка при добавлении заказов в БД. Error: {e}")
            finally:
                await conn.close()
            logger.info(
                f"get_orders: кабинет {cab['name']} обработан. "
                f"Получено={len(response)}, записано={saved_count}, пропущено={skipped_count}, "
                f"dateFrom={param['date_from']}"
            )


async def get_prices_from_lk(lk: dict):
    """
    Получаем данные о ценах прямо из личного кабинета
    Returns:
    """

    cookie_str = lk["cookie"]
    cookie_list = cookie_str.split(";")
    cookie_dict = {i.split("=")[0]: i.split("=")[1] for i in cookie_list}

    authorizev3 = lk["authorizev3"]

    proxy = "31806a1a:6846a6171a@45.13.192.129:30018"

    cookies = {
        'external-locale': 'ru',
        '_wbauid': cookie_dict["_wbauid"],
        'wbx-validation-key': cookie_dict["wbx-validation-key"],
        'WBTokenV3': authorizev3,
        'x-supplier-id-external': cookie_dict["x-supplier-id-external"],
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'ru-RU,ru;q=0.9',
        'authorizev3': authorizev3,
        'content-type': 'application/json',
        'origin': 'https://seller.wildberries.ru',
        'priority': 'u=1, i',
        'referer': 'https://seller.wildberries.ru/',
        'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="136"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    json_data = {
        'limit': 200,
        'offset': 0,
        'facets': [],
        'filterWithoutPrice': False,
        'filterWithLeftovers': False,
        'sort': 'price',
        'sortOrder': 0,
    }
    url = "https://discounts-prices.wildberries.ru/ns/dp-api/discounts-prices/suppliers/api/v1/list/goods/filter"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, cookies=cookies, json=json_data, timeout=60, #proxy=f"http://{proxy}",
                                ssl=False) as response:
                response_text = await response.text()
                try:
                    response.raise_for_status()
                    return json.loads(response_text)
                except Exception as e:
                    logger.error(
                        f"Ошибка в get_prices_from_lk: {e}.  Ответ: {response_text}"
                    )
                    return None
    except Exception as e:
        raise Exception(e)


async def get_qustions():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})
    async def get_data(cab: dict):
        """
        Получаем неотвеченный вопросы
        """
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_question",
                "API_KEY": cab["token"],
                "isAnswered": 0,
            }
            response = await wb_api(session, param)
            response = response["data"]["questions"]

            data = [
                {
                    "id_question": i["id"],
                    "nmid": i["productDetails"]["nmId"],
                    "createdDate": i["createdDate"],
                    "question": i["text"]
                }
                for i in response
            ]

            return data


    tasks = [
        get_data(cab)
        for cab in cabinets
    ]
    data = await asyncio.gather(*tasks)
    data = list(chain.from_iterable(data))

    api_ids_questions = [i["id_question"] for i in data]

    ids_db_is_not_ans = await get_data_from_db("myapp_questions", ["id_question"], {"is_answered": False})
    ids_db_is_not_ans = [i["id_question"] for i in ids_db_is_not_ans]

    ids_need_change_to_true = list(set(ids_db_is_not_ans) - set(api_ids_questions))

    if ids_need_change_to_true:
        conn = await async_connect_to_database()
        if not conn:
            logger.error("Ошибка подключения к БД в get_qustions")
            raise
        try:
            placeholders = ','.join(f'${i + 1}' for i in range(len(ids_need_change_to_true)))
            query = f"""
                UPDATE myapp_questions 
                SET
                    is_answered = TRUE
                WHERE id_question IN ({placeholders})
            """
            await conn.execute(query, *ids_need_change_to_true)
        except Exception as e:
            logger.error(
                f"Ошибка обновления отвеченных вопросов в myapp_questions. Error: {e}"
            )
            raise
        finally:
            await conn.close()

    data = [i for i in data if i["id_question"] not in ids_need_change_to_true]

    if data:
        conn = await async_connect_to_database()
        if not conn:
            logger.error("Ошибка подключения к БД в get_qustions")
            raise
        conn = await conn.acquire()
        try:
            async with conn.transaction():
                for quant in data:
                    await conn.execute(
                        """
                        INSERT INTO myapp_questions (nmid, id_question, created_at, question, answer, is_answered)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (nmid, id_question) DO NOTHING
                        """,
                        quant["nmid"], quant["id_question"], parse_datetime(quant["createdDate"]),
                        quant["question"], "", False
                    )
        except Exception as e:
            logger.error(f"Ошибка при добавлении вопросов в БД. Error: {e}")
        finally:
            await conn.close()


async def get_stock_age_by_period():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})

    async def get_analitics(cab: dict, period_get: int, id_report):
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "seller_analytics_generate",
                "API_KEY": cab["token"],
                "reportType": "STOCK_HISTORY_REPORT_CSV",
                "start": (datetime.now() + timedelta(hours=3) - timedelta(days=period_get)).strftime('%Y-%m-%d'),
                "end": (datetime.now() + timedelta(hours=3) - timedelta(days=1)).strftime('%Y-%m-%d'), #вчера с текущим временем
                "id": id_report, #'685d17f6-ed17-44b4-8a86-b8382b05873c'
                "userReportName": get_uuid(),
            }
            response = await wb_api(session, param)
            logger.info(f"Генерируем отчет для {cab['name']}. ID: {id_report}. Period: {period_get}")

            if not (response and response.get("data") and response["data"] == "Началось формирование файла/отчета"):
                logger.error(f"Ошибка формирования отчета. Период {period_get}. Кабинет: {cab['name']}. Ответ: {response}")
                raise

            for attempt in range(4):
                if attempt == 3:
                    logger.error(f"‼️Ошибка получения данных в get_stock_age_by_period. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                    raise

                await asyncio.sleep(10)
                param = {
                    "type": "seller_analytics_report",
                    "API_KEY": cab["token"],
                    "downloadId": id_report
                }

                response = await wb_api(session, param)
                if not isinstance(response, bytes):
                    await asyncio.sleep(55)
                else:
                    try:
                        text = response.decode('utf-8')
                        if "check correctness of download id or supplier id" in text:
                            await asyncio.sleep(55)
                            logger.info(f"ВНИМАНИЕ!!!: check correctness of download id or supplier id. ПОПЫТКА: {attempt + 1}. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                            continue
                        text = json.loads(text)
                        if text.get("title"):
                            await asyncio.sleep(55)
                            continue
                    except:
                        break

            with zipfile.ZipFile(io.BytesIO(response)) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as csv_file:
                        # читаем CSV построчно
                        reader = csv.reader(io.TextIOWrapper(csv_file, encoding='utf-8'))

                        data = []
                        header = next(reader)
                        OfficeMissingTime_index = header.index("OfficeMissingTime")
                        nmid_index = header.index("NmID")
                        OfficeName_index = header.index("OfficeName") # может быть пустой строкой
                        for index, row in enumerate(reader):
                            if index == 0: continue # пропускаем шапку
                            if row[OfficeName_index] == "": continue # если пустое название склада
                            data.append(
                                (
                                    int(row[nmid_index]),
                                    row[OfficeName_index].replace("Виртуальный ", "").replace("СЦ ", "").replace(" WB", "").replace(", Молодежненское", " (Молодежненское)").replace(" Сталелитейная", ""),
                                    math.floor((period_get*24-int(row[OfficeMissingTime_index]))/24) if row[OfficeMissingTime_index] not in ["-1", "-2", "-3", "-4"] else 0,
                                )
                            )

                        conn = await async_connect_to_database()
                        if not conn:
                            logger.error("Ошибка подключения к БД в add_set_data_from_db")
                            raise

                        try:
                            column_map = {
                                3: "days_in_stock_last_3",
                                7: "days_in_stock_last_7",
                                14: "days_in_stock_last_14",
                                30: "days_in_stock_last_30",
                                45: "days_in_stock_last_45"
                            }
                            column_period = column_map.get(period_get)
                            if not column_period:
                                raise ValueError(f"Неподдерживаемый период: {period_get}")

                            # Подготовка VALUES и параметров
                            values_placeholders = []
                            values_data = []
                            for idx, (nmid, warehousename, OfficeMissingTime) in enumerate(data):
                                base = idx * 3
                                values_placeholders.append(f"(${base + 1}::integer, ${base + 2}::text, ${base + 3}::integer)")
                                values_data.extend([nmid, warehousename, OfficeMissingTime])

                            query = f"""
                                UPDATE myapp_stocks AS p 
                                SET
                                    {column_period} = v.OfficeMissingTime
                                FROM (
                                    VALUES {', '.join(values_placeholders)}
                                ) AS v(nmid, warehousename, OfficeMissingTime)
                                WHERE v.nmid = p.nmid 
                                    AND p.warehousename ILIKE '%' || v.warehousename || '%'
                            """
                            await conn.execute(query, *values_data)

                        except Exception as e:
                            logger.error(
                                f"Ошибка обновления nmid, warehousename, column_period в myapp_stocks. Error: {e}"
                            )
                            raise
                        finally:
                            await conn.close()

    for period in [3, 7, 14, 30, 45]:
        tasks = []
        for cab in cabinets:
            id_report = get_uuid()  # 👉 делаем тут
            tasks.append(get_analitics(cab, period, id_report))
        await asyncio.gather(*tasks)
        await asyncio.sleep(60)


async def get_stat_products():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})

    async def get_analitics(cab: dict, period_get: int):
        async with aiohttp.ClientSession() as session:
            id_report = get_uuid()
            param = {
                "type": "seller_analytics_generate",
                "API_KEY": cab["token"],
                "reportType": "DETAIL_HISTORY_REPORT",
                "start": (datetime.now() + timedelta(hours=3) - timedelta(days=period_get)).strftime('%Y-%m-%d'),
                "end": (datetime.now() + timedelta(hours=3) - timedelta(days=1)).strftime('%Y-%m-%d'),
                "id": id_report,  # '685d17f6-ed17-44b4-8a86-b8382b05873c'
                "userReportName": get_uuid(),
            }
            response = await wb_api(session, param)

            if not (response and response.get("data") and response["data"] == "Началось формирование файла/отчета"):
                logger.error(
                    f"Ошибка формирования отчета. Период {period_get}. Кабинет: {cab['name']}. Ответ: {response}")
                raise

            for attempt in range(4):
                if attempt == 3:
                    logger.error(
                        f"‼️Ошибка получения данных в get_stat_products. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                    raise
                await asyncio.sleep(10)
                param = {
                    "type": "seller_analytics_report",
                    "API_KEY": cab["token"],
                    "downloadId": id_report
                }

                response = await wb_api(session, param)
                if not isinstance(response, bytes):
                    await asyncio.sleep(55)
                else:
                    try:
                        text = response.decode('utf-8')
                        if "check correctness of download id or supplier id" in text:
                            await asyncio.sleep(55)
                            logger.info(
                                f"ВНИМАНИЕ!!!: check correctness of download id or supplier id. ПОПЫТКА: {attempt + 1}. Кабинет {cab['name']}. ID: {id_report}. Period: {period_get}")
                            continue
                        text = json.loads(text)
                        if text.get("title"):
                            await asyncio.sleep(55)
                            continue
                    except Exception as e:
                        break
            with zipfile.ZipFile(io.BytesIO(response)) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as csv_file:
                        # читаем CSV построчно
                        reader = csv.reader(io.TextIOWrapper(csv_file, encoding='utf-8'))

                        data = []
                        header = next(reader)

                        nmid_index = header.index("nmID")
                        date_wb = header.index("dt")
                        openCardCount = header.index("openCardCount")
                        addToCartCount = header.index("addToCartCount")
                        ordersCount = header.index("ordersCount")
                        ordersSumRub = header.index("ordersSumRub")
                        buyoutsCount = header.index("buyoutsCount")
                        buyoutsSumRub = header.index("buyoutsSumRub")
                        cancelCount = header.index("cancelCount")
                        cancelSumRub = header.index("cancelSumRub")
                        addToCartConversion = header.index("addToCartConversion")
                        cartToOrderConversion = header.index("cartToOrderConversion")
                        buyoutPercent = header.index("buyoutPercent")


                        for index, row in enumerate(reader):
                            if index == 0: continue  # пропускаем шапку
                            data.append(
                                (
                                    int(row[nmid_index]),
                                    parse_datetime(row[date_wb]),
                                    int(row[openCardCount]),
                                    int(row[addToCartCount]),
                                    int(row[ordersCount]),
                                    int(row[ordersSumRub]),
                                    int(row[buyoutsCount]),
                                    int(row[buyoutsSumRub]),
                                    int(row[cancelCount]),
                                    int(row[cancelSumRub]),
                                    int(row[addToCartConversion]),
                                    int(row[cartToOrderConversion]),
                                    int(row[buyoutPercent]),
                                    int(cab['id'])
                                )
                            )

                        conn = await async_connect_to_database()
                        if not conn:
                            logger.error("Ошибка подключения к БД в get_stat_products")
                            raise

                        try:
                            BATCH_SIZE = 1000
                            for batch_start in range(0, len(data), BATCH_SIZE):
                                batch = data[batch_start:batch_start + BATCH_SIZE]
                                # Подготовка VALUES и параметров
                                values_placeholders = []
                                values_data = []

                                for idx, (
                                        nmid, date_wb, openCardCount, addToCartCount, ordersCount, ordersSumRub, buyoutsCount,
                                        buyoutsSumRub, cancelCount, cancelSumRub, addToCartConversion, cartToOrderConversion,
                                        buyoutPercent, lk) in enumerate(batch):
                                    base = idx * 14
                                    values_placeholders.append(
                                        f"(${base + 1}::integer, ${base + 2}, ${base + 3}::integer, "
                                        f"${base + 4}::integer, ${base + 5}::integer, ${base + 6}::integer, "
                                        f"${base + 7}::integer, ${base + 8}::integer, ${base + 9}::integer, "
                                        f"${base + 10}::integer, ${base + 11}::integer, ${base + 12}::integer, "
                                        f"${base + 13}::integer, ${base + 14}::integer)"
                                    )
                                    values_data.extend([
                                        nmid, date_wb, openCardCount, addToCartCount, ordersCount, ordersSumRub,
                                        buyoutsCount, buyoutsSumRub, cancelCount, cancelSumRub,
                                        addToCartConversion, cartToOrderConversion, buyoutPercent, lk
                                    ])

                                query = f"""
                                    INSERT INTO myapp_productsstat (
                                        nmid, date_wb, "openCardCount", "addToCartCount", "ordersCount", "ordersSumRub",
                                        "buyoutsCount", "buyoutsSumRub", "cancelCount", "cancelSumRub",
                                        "addToCartConversion", "cartToOrderConversion", "buyoutPercent", "lk_id"
                                    )
                                    VALUES {', '.join(values_placeholders)}
                                    ON CONFLICT (nmid, date_wb, lk_id) DO UPDATE SET
                                        "openCardCount" = EXCLUDED."openCardCount",
                                        "addToCartCount" = EXCLUDED."addToCartCount",
                                        "ordersCount" = EXCLUDED."ordersCount",
                                        "ordersSumRub" = EXCLUDED."ordersSumRub",
                                        "buyoutsCount" = EXCLUDED."buyoutsCount",
                                        "buyoutsSumRub" = EXCLUDED."buyoutsSumRub",
                                        "cancelCount" = EXCLUDED."cancelCount",
                                        "cancelSumRub" = EXCLUDED."cancelSumRub",
                                        "addToCartConversion" = EXCLUDED."addToCartConversion",
                                        "cartToOrderConversion" = EXCLUDED."cartToOrderConversion",
                                        "buyoutPercent" = EXCLUDED."buyoutPercent";
                                """
                                await conn.execute(query, *values_data)

                        except Exception as e:
                            logger.error(
                                f"Ошибка обновления данных в myapp_productsstat. Error: {e}"
                            )
                            raise
                        finally:
                            await conn.close()
    tasks = [get_analitics(cab, 7) for cab in cabinets]
    await asyncio.gather(*tasks)


async def get_supplies():
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'groups_id': 1})
    async def get_analitics(cab, period_get: int):
        async with aiohttp.ClientSession() as session:
            param = {
                "type": "get_delivery_fbw",
                "API_KEY": cab["token"],
                "dateFrom": (datetime.now() + timedelta(hours=3) - timedelta(days=period_get)).strftime('%Y-%m-%d')
            }
            response = await wb_api(session, param)
            data = [
                (
                    i["nmId"], i["incomeId"], i["number"], parse_datetime(i["date"]), parse_datetime(i["lastChangeDate"]),
                    i["supplierArticle"], i["techSize"], i["barcode"], i["quantity"], i["totalPrice"], parse_datetime(i["dateClose"]),
                    i["warehouseName"], i["status"]
            )
                for i in response
                if i["status"] == "Принято"
            ]
            conn = await async_connect_to_database()
            if not conn:
                logger.error("Ошибка подключения к БД")
                raise
            try:
                query = f"""
                    INSERT INTO myapp_supplies (
                        nmid, "incomeId", "number", "date_post", "lastChangeDate", "supplierArticle",
                        "techSize", "barcode", "quantity", "totalPrice",
                        "dateClose", "warehouseName", "status"
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7, $8, $9, $10, $11,
                        $12, $13
                    )
                    ON CONFLICT (nmid, "incomeId") DO UPDATE SET
                        "number" = EXCLUDED."number",
                        "date_post" = EXCLUDED."date_post",
                        "lastChangeDate" = EXCLUDED."lastChangeDate",
                        "supplierArticle" = EXCLUDED."supplierArticle",
                        "techSize" = EXCLUDED."techSize",
                        "barcode" = EXCLUDED."barcode",
                        "quantity" = EXCLUDED."quantity",
                        "totalPrice" = EXCLUDED."totalPrice",
                        "dateClose" = EXCLUDED."dateClose",
                        "warehouseName" = EXCLUDED."warehouseName",
                        "status" = EXCLUDED."status";
                """
                await conn.executemany(query, data)
            except Exception as e:
                logger.error(
                    f"Ошибка обновления данных в myapp_supplies. Error: {e}"
                )
                raise
            finally:
                await conn.close()



    tasks = [get_analitics(cab, 7) for cab in cabinets]
    await asyncio.gather(*tasks)

# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(test_addv())


