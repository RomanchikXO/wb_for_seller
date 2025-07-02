from database.DataBase import async_connect_to_database
from google.functions import fetch_google_sheet_data
import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


async def get_cost_price_from_google():
    url = "https://docs.google.com/spreadsheets/d/19hbmos6dX5WGa7ftRagZtbCVaY-bypjGNE2u0d9iltk/edit?gid=1431573654#gid=1431573654"
    data = fetch_google_sheet_data(
        url,
        "Себесы",
    )
    try:
        data = [(i[0].lower(), float(i[1].replace(",", ".").replace("\xa0", ""))) for i in data[1:]]
    except Exception as e:
        logger.error(f"Ошибка обработки данных в get_cost_price_from_google. Ошибка {e}")
        raise

    conn = await async_connect_to_database()
    if not conn:
        logger.error(f"Ошибка подключения к БД в set_wallet_discount")
        return
    try:
        values_clause = ", ".join(
            f"('{vendorcode}', {cost_price})" for vendorcode, cost_price in data
        )
        request = f"""
                UPDATE myapp_price AS p
                SET 
                    cost_price = v.cost_price
                FROM (VALUES 
                    {values_clause}
                    ) AS v(vendorcode, cost_price)
                WHERE LOWER(v.vendorcode) = LOWER(p.vendorcode)
            """
        await conn.execute(request)
    except Exception as e:
        logger.error(f"Ошибка обновления cost_price в myapp_price. Запрос {request}. Error: {e}")
    finally:
        await conn.close()

# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(get_cost_price_from_google())