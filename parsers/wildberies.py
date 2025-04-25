import asyncio
import random
import httpx
from typing import Union
import time
from celery_app.celery_config import logger
import aiohttp

from database.funcs_db import get_data_from_db, add_set_data_from_db

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
    link: Union[int, str],
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

    card_price = calculate_card_price(promo_price) if promo_price else calculate_card_price(price)
    return card_price

def safe_parse_link(link):
    try:
        data = parse_link(link)
        return data
    except Exception as e:
        logger.error(f"Can't parse link. Url: {link}. Error: {e}")

def parse_by_links(links: list) -> list:
    tasks = [
        safe_parse_link(link)
        for link in links
    ]
    return tasks


def parse(links: list) -> list:
    response = parse_by_links(links)
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
        API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        params = {
            "dateFrom": param["date_from"],
            "flag": param["flag"]
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
                "begin": param['begin'],
                "end": param['end'],
            },
            "page": 1
        }
        view = 'post'

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
    cabinets = await get_data_from_db("myapp_wblk", ["id", "name", "token"], conditions={'group': 1})

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
        for key, value in id_to_result.values():
            value = value["data"]["listGoods"]
            data = []
            try:
                for item in value:
                    data.append(
                        add_set_data_from_db(
                            table_name="myapp_price",
                            data=dict(
                                lk=key,
                                nmID=item["nmID"],
                                vendorCode=item["vendorCode"],
                                sizes=item["sizes"],
                                discount=item["discount"],
                                clubDiscount=item["clubDiscount"],
                                editableSizePrice=item["editableSizePrice"],
                            ),
                            conflict_fields=["nmID", "lk"]
                        )
                    )
                results = await asyncio.gather(*data)
            except Exception as e:
                logger.error(f"Ошибка при добавлении продуктов и цен {e}")

# loop = asyncio.get_event_loop()
# loop.run_until_complete(get_products_and_prices())