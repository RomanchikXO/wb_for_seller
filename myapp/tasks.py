from celery import shared_task
import asyncio

from parsers.wildberies import get_nmids, get_stocks_data_2_weeks, get_orders
from tasks.google_our_prices import set_prices_on_google, get_products_and_prices, get_black_price_spp
from tasks.set_price_on_wb_from_repricer import set_price_on_wb_from_repricer
from tasks.google_podsort import set_orders_quantity_in_google
from tasks.google_wb_prices import process_data

import logging
from decorators import with_task_context
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("myapp"))


@shared_task
@with_task_context("update_prices_on_google")
def update_prices_on_google():
    logger.info("🟢 Устанавливаем цены в гугл таблицу")
    asyncio.run(set_prices_on_google())
    logger.info("Цены в гугл таблицу установлены")


@shared_task
@with_task_context("get_prices_and_products")
def get_prices_and_products():
    logger.info("🟢 Собираем товары и цены в БД")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")


@shared_task
@with_task_context("some_task")
def some_task():
    import requests

    cookies = {
        'external-locale': 'ru',
        'wbx-validation-key': '892202ae-946c-49ce-bc5a-197254fe0855',
        '_wbauid': '3066622261747143449',
        'x-supplier-id-external': '6ce0b1b3-c8e4-4cc6-a8ca-52df548e9925',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'ru,en;q=0.9,pl;q=0.8,ko;q=0.7',
        'authorizev3': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDcxNDM1MDMsInVzZXIiOiIyMjg4MTE2NCIsInNoYXJkX2tleSI6IjExIiwiY2xpZW50X2lkIjoic2VsbGVyLXBvcnRhbCIsInNlc3Npb25faWQiOiI4NmMyZjc3OTk5ZGQ0MDAxYjZhYzk0NzEyY2U4NmFjZCIsInZhbGlkYXRpb25fa2V5IjoiMTc4MmU2YTc0NDk2OTdjN2MwNjYzMzFkZDRiNDc1Yzk4N2JlZTkyOWI1MzYzYzAyM2QyMDdlYzFhMmEwMTcyOCIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc1Mjk3NTcwLCJ2ZXJzaW9uIjoyfQ.Vt2eUgW2bhwxOZApl0lIjO2Gks8Yy2SiiZUxWb1YAYirHPP1UcRylHnYT8fmc8cuBZ4jNUOjexuW0CHdKr-zXIoETrxlM11yk9t4afyoWrQKoQjt4dgAaYNxjEuBKGyL57it2BBSzjwreO52euBIFBSwgaU_udkf2t2vD5baCNUf3C9hWgt1uK256czqfbKLxPQ3XsUxzhF2ES-0CRKjHsEHBJUgvphASkU42T3zbl4ElPW-kzgfFKucYMceS6Ohc4EycXR03bA0Fw6-b2baVAa6WnIrxndPXLkh3JznuFdP1Vpc6VMH8-PZq3XocZ0F0C9qeFls08sMmDQOinqGLQ',
        'content-type': 'application/json',
        'dnt': '1',
        'origin': 'https://seller.wildberries.ru',
        'priority': 'u=1, i',
        'referer': 'https://seller.wildberries.ru/',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "YaBrowser";v="25.4", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 YaBrowser/25.4.0.0 Safari/537.36',
    }

    json_data = {
        'limit': 50,
        'offset': 0,
        'facets': [],
        'filterWithoutPrice': False,
        'filterWithLeftovers': False,
        'sort': 'price',
        'sortOrder': 0,
    }

    response = requests.post(
        'https://discounts-prices.wildberries.ru/ns/dp-api/discounts-prices/suppliers/api/v1/list/goods/filter',
        cookies=cookies,
        headers=headers,
        json=json_data,
    )

    logger.info(response.text)
    return "test"


@shared_task
@with_task_context("prices_table")
def prices_table():
    url_prices = "https://docs.google.com/spreadsheets/d/1EhjutxGw8kHlW1I3jbdgD-UMA5sE20aajMO865RzrlA/edit?gid=1101267699#gid=1101267699"
    logger.info("🟢 Обновляем гугл таблицу с ценами конкурентов и доходом")
    process_data(url_prices)
    logger.info("Гугл таблица с ценами конкурентов и доходом обновлена ")


@shared_task
@with_task_context("get_nmids_to_db")
def get_nmids_to_db():
    logger.info("🟢 Обновляем таблицу со всеми артикулами в бд")
    asyncio.run(get_nmids())
    logger.info("Таблица со всеми артикулами обновлена")


@shared_task
@with_task_context("get_stocks_to_db")
def get_stocks_to_db():
    logger.info("🟢 Обновляем таблицу с остатками товаров на складах в бд")
    asyncio.run(get_stocks_data_2_weeks())
    logger.info("Таблица с остатками товаров на складах обновлена")


@shared_task
@with_task_context("get_orders_to_db")
def get_orders_to_db():
    logger.info("🟢 Обновляем таблицу с заказами в бд")
    asyncio.run(get_orders())
    logger.info("Таблица с заказами в бд обновлена")


@shared_task
@with_task_context("get_set_ord_quant_to_google")
def get_set_ord_quant_to_google():
    logger.info("🟢 Обновляем остатки и заказы в гугл таблице")
    asyncio.run(set_orders_quantity_in_google())
    logger.info("Остатки и заказы в гугл таблице обновлены")


@shared_task
@with_task_context("set_black_price_spp_on_db")
def set_black_price_spp_on_db():
    logger.info("🟢 Обновляем spp и blackprice в БД")
    asyncio.run(get_black_price_spp())
    logger.info("✅ spp и blackprice в БД обновлены")

    logger.info("🟢 Обновляем цену в репрайсере")
    asyncio.run(set_price_on_wb_from_repricer())
    logger.info("✅ Цены обновлены")