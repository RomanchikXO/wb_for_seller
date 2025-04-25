from celery_app.celery_config import app, logger
from parsers.wildberies import get_products_and_prices
import asyncio


@app.task
def get_prices_and_products():
    logger.info("Собираем товары и цены")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")