import json

from celery_app.celery_config import app, logger
from google.functions import fetch_google_sheet_data, update_google_prices_data_with_format
from parsers.wildberies import get_products_and_prices, parse
import asyncio
from database.funcs_db import get_data_from_db

# Все таски находятся внизу !!!


async def set_prices_on_google():
    """
    - Получаем nmid и цену из бд
    - Получаем данные из гугл таблицы
    - Сопостовляем и обновляем гугл таблицу
    - Записываем данные в гугл таблицу
    :return:
    """

    url = "https://docs.google.com/spreadsheets/d/1PEhnRK9k8z8rMyeFCZT_WhO4DYkgjrqQgqLhE7XlTfA/edit?gid=1041007463#gid=1041007463"

    data = await get_data_from_db(
        table_name="myapp_price",
        columns=["nmid", "sizes"],
        additional_conditions="EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_price.lk_id AND myapp_wblk.groups_id = 1)"
    )
    result_dict = {item['nmid']: json.loads(item['sizes']) for item in data}

    google_data = fetch_google_sheet_data(
        spreadsheet_url=url,
        sheet_identifier=9,

    )
    discount = None
    for index, _string in enumerate(google_data):
        nmID = _string[2]

        if not discount:
            prices_parse = parse([nmID], ["promo_price", "price"])
            if prices_parse[0] and all(prices_parse[0]):
                discount = round(abs(((prices_parse[0][1] / (prices_parse[0][0] * 0.1)) - 1) * 100))

        if sizes:=result_dict.get(nmID):
            price = sizes[0]["price"]
            google_data[index][8] = price

    url = "https://docs.google.com/spreadsheets/d/1PEhnRK9k8z8rMyeFCZT_WhO4DYkgjrqQgqLhE7XlTfA/edit?gid=1041007463#gid=1041007463"
    update_google_prices_data_with_format(
        url, int(url.split("=")[-1]), 0, 0, google_data, {"discount": discount}
    )






@app.task
def update_prices_on_google():
    logger.info("Тестовая функция стартовала")
    asyncio.run(set_prices_on_google())
    logger.info("Тестовая функция завершена")


@app.task
def get_prices_and_products():
    logger.info("Собираем товары и цены")
    asyncio.run(get_products_and_prices())
    logger.info("Товары и цены собраны в БД")