import json
import math
import asyncio

from database.DataBase import async_connect_to_database
from google.functions import fetch_google_sheet_data, update_google_prices_data_with_format
from parsers.wildberies import get_products_and_prices, parse, get_prices_from_lk
from database.funcs_db import get_data_from_db

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))

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
        columns=["nmid", "sizes", "discount", "spp"],
        additional_conditions="EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_price.lk_id AND myapp_wblk.groups_id = 1)"
    )
    result_dict = {
        item['nmid']: {
            "sizes": json.loads(item["sizes"]),
            "discount": item["discount"],
            "spp": item["spp"],
        }
        for item in data
    }

    google_data = fetch_google_sheet_data(
        spreadsheet_url=url,
        sheet_identifier=9,

    )


    for index, _string in enumerate(google_data):
        if index == 0: continue
        nmID = int(_string[2])

        if nm_info:=result_dict.get(nmID):
            price = int(nm_info["sizes"][0]["price"])
            discount_table = int(nm_info["discount"])
            spp_table = str(nm_info['spp']) + "%"
            google_data[index][8] = price
            google_data[index][10] = discount_table
            google_data[index][11] = spp_table
        else:
            google_data[index][8] = "0"
            google_data[index][10] = "0"
            google_data[index][11] = "0%"

    update_google_prices_data_with_format(
        url, int(url.split("=")[-1]), 0, 0, google_data
    )

async def get_black_price_spp():
    conn = await async_connect_to_database()

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        return

    request = ("SELECT cookie, authorizev3 "
               "FROM myapp_wblk "
               "WHERE groups_id = 1")
    try:
        all_fields = await conn.fetch(request)
        lks = [
            {
                "cookie": row["cookie"],
                "authorizev3": row["authorizev3"]
            }
            for row in all_fields
        ]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_wblk в get_black_price_spp. Запрос {request}. Error: {e}")
    finally:
        await conn.close()

    data = (get_prices_from_lk(lk) for lk in lks)
    response = await asyncio.gather(*data)


    try:
        updates = {
            nmid["nmID"]: {
                "blackprice": round((nmid["discountedPrices"][0] / 100) * (100 - (nmid.get("discountOnSite") or 0))),
                "spp": nmid.get("discountOnSite") or 0,
                "redprice": math.floor(round((nmid["discountedPrices"][0] / 100) * (100 - (nmid.get("discountOnSite") or 0))) * 0.97)
            }
            for item in response
            for nmid in item["data"]["listGoods"]
        }
    except Exception as e:
        logger.error(f"Ошибка: {e}. Response: {response}")
        return

    values = [(nmid, data["blackprice"], data["spp"], data["redprice"]) for nmid, data in updates.items()]

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в add_set_data_from_db")
        return
    try:
        query = """
            UPDATE myapp_price AS p 
            SET
                blackprice = v.blackprice,
                spp = v.spp,
                redprice = v.redprice
            FROM (VALUES
                {}
            ) AS v(nmid, blackprice, spp, redprice)
            WHERE v.nmid = p.nmid
        """.format(", ".join(
            f"({nmid}, {blackprice}, {spp}, {redprice})" for nmid, blackprice, spp, redprice in values
        ))
        await conn.execute(query)
    except Exception as e:
        logger.error(f"Ошибка обновления spp и blackprice в myapp_price. Error: {e}")
    finally:
        await conn.close()

# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(get_black_price_spp())