from celery import shared_task
from celery.utils.log import get_task_logger
import asyncio

from parsers.wildberies import get_nmids, get_stocks_data_2_weeks, get_orders
from tasks.google_our_prices import set_prices_on_google, get_products_and_prices, get_black_price_spp
from tasks.google_podsort import set_orders_quantity_in_google
from tasks.google_wb_prices import process_data


logger = get_task_logger("myapp")

@shared_task
def update_prices_on_google():
    logger.info("🟢 Устанавливаем цены в гугл таблицу")
    asyncio.run(set_prices_on_google())
    logger.info("Цены в гугл таблицу установлены")


@shared_task
def get_prices_and_products():
    logger.info("🟢 Собираем товары и цены в БД")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")


@shared_task
def some_task():
    logger.info("🟢 Тестируем. Ща вернет 'test' или не вернет")
    return "test"


@shared_task
def prices_table():
    url_prices = "https://docs.google.com/spreadsheets/d/1EhjutxGw8kHlW1I3jbdgD-UMA5sE20aajMO865RzrlA/edit?gid=1101267699#gid=1101267699"
    logger.info("🟢 Обновляем гугл таблицу с ценами конкурентов и доходом")
    process_data(url_prices)
    logger.info("Гугл таблица с ценами конкурентов и доходом обновлена ")


@shared_task
def get_nmids_to_db():
    logger.info("🟢 Обновляем таблицу со всеми артикулами в бд")
    asyncio.run(get_nmids())
    logger.info("Таблица со всеми артикулами обновлена")


@shared_task
def get_stocks_to_db():
    logger.info("🟢 Обновляем таблицу с остатками товаров на складах в бд")
    asyncio.run(get_stocks_data_2_weeks())
    logger.info("Таблица с остатками товаров на складах обновлена")


@shared_task
def get_orders_to_db():
    logger.info("🟢 Обновляем таблицу с заказами в бд")
    asyncio.run(get_orders())
    logger.info("Таблица с заказами в бд обновлена")


@shared_task
def get_set_ord_quant_to_google():
    logger.info("🟢 Обновляем остатки и заказы в гугл таблице")
    asyncio.run(set_orders_quantity_in_google())
    logger.info("Остатки и заказы в гугл таблице обновлены")


@shared_task
def set_black_price_spp_on_db():
    logger.info("🟢 Обновляем spp и blackprice в БД")
    asyncio.run(get_black_price_spp())
    logger.info("spp и blackprice в БД обновлены")