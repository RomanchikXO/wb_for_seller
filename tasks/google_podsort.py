import asyncio
from datetime import datetime, timedelta

from database.DataBase import async_connect_to_database
from google.functions import fetch_google_sheet_data, update_google_sheet_data

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


async def get_orders_in_db() -> dict:
    conn = await async_connect_to_database()
    date_from = str((datetime.now() + timedelta(hours=3) - timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0))
    date_to = str((datetime.now() + timedelta(hours=3)).replace(hour=0, minute=0, second=0, microsecond=0))

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        raise
    try:
        request = (f"SELECT supplierarticle, COUNT(*) AS total "
                   f"FROM myapp_orders "
                   f"WHERE date >= '{date_from}' AND date < '{date_to}' "
                   f"AND EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_orders.lk_id AND myapp_wblk.groups_id = 1)"
                   f"GROUP BY supplierarticle")
        all_fields = await conn.fetch(request)
        result = {row["supplierarticle"]: row["total"] for row in all_fields}
        return result
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_orders. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


async def get_quantity_in_db(supplierarticles: list) -> dict:
    conn = await async_connect_to_database()

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        raise
    try:
        request = ("SELECT supplierarticle, sum(quantity) AS total "
                   "FROM myapp_stocks "
                   "WHERE supplierarticle = ANY($1) "
                   "AND EXISTS (SELECT 1 FROM myapp_wblk WHERE myapp_wblk.id = myapp_stocks.lk_id AND myapp_wblk.groups_id = 1)"
                   "GROUP BY supplierarticle")
        all_fields = await conn.fetch(request, supplierarticles)
        result = {row["supplierarticle"]: row["total"] for row in all_fields}
        return result
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_stocks. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


async def set_orders_quantity_in_google()->None:
    """
    Получаем заказы из БД
    Получаем остатки из БД
    Получаем данные из гугл таблицы
    Обновляем данные гугл таблицы и записываем обратно
    Returns:

    """
    url = "https://docs.google.com/spreadsheets/d/1zsuNRaHFiYwfp-YuHtxJYxsI5Htt05PyOv0yo9mJrdA/edit?gid=362718067#gid=362718067"

    orders = await get_orders_in_db()
    articles = list(orders.keys())

    quantity = await get_quantity_in_db(articles)

    orders_to_table = []
    quantity_to_table = []

    google_data = fetch_google_sheet_data(url, 2)
    for row in google_data[2:]: # итерация по строкам начиная со второй
        ord_new = int(orders.get(row[0], 0))
        quantity_new = int(quantity.get(row[0], 0))

        orders_to_table.append([ord_new])
        quantity_to_table.append([quantity_new])



    try:
        update_google_sheet_data(url, 2, f"C3:C{len(orders_to_table)+3}", orders_to_table)
        update_google_sheet_data(url, 2, f"G3:G{len(quantity_to_table) + 3}", quantity_to_table)
    except Exception as e:
        logger.error(f"Ошибка обновления таблицы с остатками и заказами. Error: {e}")


# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(set_orders_quantity_in_google())