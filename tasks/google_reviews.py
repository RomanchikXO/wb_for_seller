from datetime import datetime, timedelta
from typing import List
from database.DataBase import async_connect_to_database
from parsers.wildberies import wb_api
import aiohttp
import asyncio
from google.functions import update_google_sheet_data

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))


async def get_tokens() -> List[dict]:
    conn = await async_connect_to_database()

    try:
        if not conn:
            raise (f"Ошибка подключения к БД в fetch_data__get_adv_id")

        request = ("SELECT name, token "
                   "FROM myapp_wblk")
        all_fields = await conn.fetch(request)
        result = [
            {
                "name": row["name"],
                "token": row["token"]
            } for row in all_fields
        ]
    except Exception as e:
        logger.error(f"Ошибка получения токенов в get_tokens: {e}")
    finally:
        await conn.close()

    return result


async def get_feedback_from_wb(lk: dict) -> List[dict]:

    data = []
    param = {
        "type": "get_feedback",
        "API_KEY": lk["token"],
        "isAnswered": "True",
        "take": 5000,
        "skip": 0,
        "order": "dateDesc",
        # "dateFrom": 1719781200,
        # "dateTo": 1735678799,
    }

    try:
        async with aiohttp.ClientSession() as session:
            counter = 0
            while counter < 5:
                try:
                    while True:
                        result = await wb_api(session, param)
                        if result.get("data"):
                            await asyncio.sleep(1)
                            count_feedback = len(result["data"]["feedbacks"])
                            if count_feedback != 0:
                                param["skip"] += 5000
                                data += result["data"]["feedbacks"]
                            else:
                                if param["isAnswered"] == "False":
                                    return {lk["name"]: data}
                                param["skip"] = 0
                                param["isAnswered"] = "False"
                        else:
                            logger.error(f"Ошибка в get_feedback_from_wb, вот что вернулось: {result}")
                            return
                except Exception as e:
                    logger.error(f"Ошибка вызова wb_api в get_feedback_from_wb на попытке {counter + 1}: {e}")

                counter += 1
                await asyncio.sleep(1)
            logger.warning(
                f"Не удалось получить бюджет ЛК в get_feedback_from_wb после {counter} попыток. Параметры: {lk}")

    except Exception as e:
        logger.error(f"Ошибка в add_budget_advert: {e}, параметры: {lk}")


def add_feedback_to_google_table(data: list, range: str, index=0) -> None:
    # Задайте параметры таблицы
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1XfhNKjNbFTXoZesmn65xWKtV76Sx64OgA5ggTRSp8eE/edit?hl=ru&gid=0#gid=0"
    SHEET_IDENTIFIER = index  # Индекс листа (начинается с 0) или его имя (например, "Лист1")
    DATA_RANGE = range  # Пример:"A1:C2" Укажите диапазон или оставьте None для всего листа

    try:
        res = update_google_sheet_data(SPREADSHEET_URL, SHEET_IDENTIFIER, DATA_RANGE, data)
    except Exception as e:
        logger.error(f"Ошибка в add_feedback_to_google_table: {e}")


async def fetch_data__get_feedback():

    try:
        lks = await get_tokens()

        task_get_feedback = []
        results_task_get_feedback = []
        if lks:
            for lk in lks:
                task_get_feedback.append(get_feedback_from_wb(lk))
            results_task_get_feedback = await asyncio.gather(*task_get_feedback)

            data_for_table = [
                [
                    "Продавец", "Артикул", "Отзыв", "Оценка товара",
                    "Дата", "Статус", "Артикул продавца"
                ]
            ]
            for index, sub_data in enumerate(results_task_get_feedback):
                for name, value in sub_data.items():
                    for value_cur in value:
                        try:
                            prod_details = value_cur["productDetails"]
                            utc_time = datetime.strptime(value_cur["createdDate"], "%Y-%m-%dT%H:%M:%SZ")
                            moscow_time = utc_time + timedelta(hours=3)
                            moscow_time_str = moscow_time.strftime("%Y-%m-%dT%H:%M:%S")
                            data_for_table.append(
                                [
                                    name, prod_details["nmId"], value_cur["text"], value_cur["productValuation"],
                                    moscow_time_str, "Новый" if value_cur["state"]=="none" else "Обработан",
                                    prod_details.get("supplierArticle", "")
                                 ]
                            )
                        except Exception as e:
                            print(e, prod_details["productName"])
                            raise
            range = f"A1:G{len(data_for_table)+1}"
            add_feedback_to_google_table(data_for_table, range, 1)


    except Exception as e:
        logger.warning(f"Произошла ошибка в fetch_data__get_feedback: {e}")
