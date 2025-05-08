import asyncio

from database.DataBase import async_connect_to_database
import logging
from context_logger import ContextLogger
from typing import List
from parsers.wildberies import wb_api
import aiohttp

logger = ContextLogger(logging.getLogger("core"))


async def get_price_from_db_dor_wb()->List[dict]:
    """
    Получить товары которым надо устанавливать цену
    Returns:

    """

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в set_price_wb")
        return
    try:
        query = """
            SELECT 
                rp.nmid as nmid, 
                wblk.token as token, 
                rp.keep_price as keep_price,
                price.redprice as redprice,
                price.spp as spp,
                price.discount as discount
            FROM myapp_repricer rp
            INNER JOIN myapp_wblk wblk ON wblk.id = rp.lk_id 
            INNER JOIN myapp_price price ON price.nmid = rp.nmid
            WHERE 
                rp.is_active IS TRUE 
                AND rp.keep_price != price.redprice
        """
        rows = await conn.fetch(query)
        columns = [dict(row) for row in rows]
        return columns
    except Exception as e:
        logger.error(f"Ошибка получения цен из БД для репрайсера. Error: {e}")
    finally:
        await conn.close()


def set_current_list(data: List[dict])-> dict:
    response = {}

    try:
        for i in data:
            if not response.get(i["token"]):
                response[i["token"]] = []
            response[i["token"]].append(
                {
                    "nmID":int(i["nmid"]),
                    "price": int(i["keep_price"] * 1.03 * (i["spp"] / 100 + 1) * (i["discount"] / 100 + 1)),
                    "discount": int(i["discount"]),
                }
            )
    except Exception as e:
        raise f"в set_current_list: {e}"
    return response


async def set_price_on_wb_from_repricer():
    result = await get_price_from_db_dor_wb()

    if not result:
        logger.info("Отсутствуют товары для установки цен")
        return

    try:
        articles = set_current_list(result)
    except Exception as e:
        logger.error(f"Новые цены не установлены. Ошибка: {e}")
        return

    param = [
        {
            "API_KEY": key,
            "type": "set_price_and_discount",
            "data": value,
        }
        for key, value in articles.items()
    ]

    request = {}

    try:
        async with aiohttp.ClientSession() as session:
            for seller in param:
                request[seller["API_KEY"]] = wb_api(session, seller)
            results = await asyncio.gather(*request.values())
    except Exception as e:
        logger.error(f"Цены не установлены. Ошибка: {e}")
