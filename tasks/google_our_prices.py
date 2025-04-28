import json

from celery.utils.log import get_task_logger
from google.functions import fetch_google_sheet_data, update_google_prices_data_with_format
from parsers.wildberies import get_products_and_prices, parse
from database.funcs_db import get_data_from_db


logger = get_task_logger("core")

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
        columns=["nmid", "sizes", "discount"],
        additional_conditions="EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_price.lk_id AND myapp_wblk.groups_id = 1)"
    )
    result_dict = {
        item['nmid']: {
            "sizes": json.loads(item["sizes"]),
            "discount": item["discount"]
        }
        for item in data
    }

    google_data = fetch_google_sheet_data(
        spreadsheet_url=url,
        sheet_identifier=9,

    )
    discount = None
    for index, _string in enumerate(google_data):
        nmID = _string[2]

        if not discount:
            if index != 0:
                prices_parse = parse([nmID], ["promo_price", "price"])
                if prices_parse[0] and all(prices_parse[0]):
                    discount = round(abs(((prices_parse[0][1] / (prices_parse[0][0] * 0.1)) - 1) * 100))
                    logger.info(f"Discount is {discount} set.")

        if nm_info:=result_dict.get(nmID):
            price = int(nm_info["sizes"][0]["price"])
            discount_table = int(nm_info["discount"])
            google_data[index][8] = price
            google_data[index][10] = discount_table

    url = "https://docs.google.com/spreadsheets/d/1PEhnRK9k8z8rMyeFCZT_WhO4DYkgjrqQgqLhE7XlTfA/edit?gid=1041007463#gid=1041007463"
    update_google_prices_data_with_format(
        url, int(url.split("=")[-1]), 0, 0, google_data, **{"discount": discount}
    )

