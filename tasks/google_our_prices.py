from celery_app.celery_config import app, logger
from parsers.wildberies import get_products_and_prices
import asyncio
from database.funcs_db import get_data_from_db

# Все таски находятся внизу !!!


async def set_prices_on_google():
    data = await get_data_from_db(
        table_name="myapp_price",
        columns=["vendorcode", "sizes"],
        additional_conditions="EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.lk_id = myapp_price.lk_id AND myapp_wblk.groups_id = 1)"
    )
    logger.info(data)

@app.task
def test_func():
    logger.info("Тестовая функция стартовала")
    asyncio.run(set_prices_on_google())
    logger.info("Тестовая функция завершена")


@app.task
def get_prices_and_products():
    logger.info("Собираем товары и цены")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")