from celery import shared_task
from celery.utils.log import get_task_logger
import asyncio
from tasks.google_our_prices import set_prices_on_google, get_products_and_prices
from tasks.google_wb_prices import process_data


logger = get_task_logger("myapp")

@shared_task
def update_prices_on_google():
    logger.info("Устанавливаем цены в гугл таблицу")
    asyncio.run(set_prices_on_google())
    logger.info("Цены в гугл таблицу установлены")


@shared_task
def get_prices_and_products():
    logger.info("Собираем товары и цены")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")


@shared_task
def some_task():
    logger.info("Тестируем. Ща вернет 'test'")
    return "test"


@shared_task
def prices_table():
    url_prices = "https://docs.google.com/spreadsheets/d/1EhjutxGw8kHlW1I3jbdgD-UMA5sE20aajMO865RzrlA/edit?gid=1101267699#gid=1101267699"
    logger.info("Обновляем гугл таблицу с ценами конкурентов и доходом")
    process_data(url_prices)
    logger.info("Таблица обновлена ")