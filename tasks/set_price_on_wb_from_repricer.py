import asyncio
import math

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
                price.discount as discount,
                price.wallet_discount as wallet_discount
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
                    "price": math.ceil(math.ceil(math.ceil(i["keep_price"] / (100-int(i["wallet_discount"])) * 100) / (100 - i["spp"]) * 100) / (100 - i["discount"]) * 100),
                    "black_price": math.ceil(i["keep_price"] / (100-int(i["wallet_discount"])) * 100),
                    "discount": int(i["discount"]),
                    "keep_price": i["keep_price"],
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
        combined = sum(articles.values(), [])  # получаем массив со словарями [{}, {}]
        articles = {
            k: [{k2: v2 for k2, v2 in d.items() if k2 not in ["keep_price", "black_price"]} for d in v]
            for k, v in articles.items()
        }
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
    if not param:
        logger.info("Нет товаров для обновления цены")
        return

    request = {}

    try:
        async with aiohttp.ClientSession() as session:
            for seller in param:
                request[seller["API_KEY"]] = wb_api(session, seller)
            await asyncio.gather(*request.values())
    except Exception as e:
        logger.error(f"Цены не установлены. Ошибка: {e}")
        return

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в set_price_on_wb_from_repricer")
        return
    try:
        values = [(item["nmID"], item["keep_price"], item["price"], item["black_price"]) for item in combined]
        groups = []
        for idx in range(len(values)):
            # base — сдвиг для этой тройки
            base = idx * 3
            groups.append(f"(${base+1}::integer, ${base+2}::numeric, ${base+3}::numeric), ${base+4}::numeric)")
        row_placeholders = ", ".join(groups)
        flat_params = [x for triple in values for x in triple]
        query = f"""
            UPDATE myapp_price AS mp
            SET 
              redprice = d.keep_price,
              sizes = (
                SELECT jsonb_agg(
                  jsonb_set(elem, '{{price}}', to_jsonb(d.price), false)
                )
                FROM jsonb_array_elements(mp.sizes) AS elem
              ),
              blackprice = d.black_price
            FROM (
              VALUES
                {row_placeholders}
            ) AS d(nmid, keep_price, price, black_price)
            WHERE mp.nmid = d.nmid;
        """

        await conn.execute(query, *flat_params)

    except Exception as e:
        logger.error(f"Ошибка обновления цен в БД myapp_price после репрайсинга. Error: {e}")
    finally:
        await conn.close()

