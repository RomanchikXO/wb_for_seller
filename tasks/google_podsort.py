from datetime import datetime, timedelta

from celery.utils.log import get_task_logger

from database.DataBase import async_connect_to_database


logger = get_task_logger("core")


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
        request = ("SELECT supplierarticle, COUNT(*) AS total "
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


async def set_orders_quantity_in_google():
    orders = await get_orders_in_db()
    articles = list(orders.keys())

    quantity = await get_quantity_in_db(articles)

    logger.info(f"Заказы: {orders}")
    logger.info(f"Остатки: {quantity}")


# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(get_orders())