import json
from database.DataBase import async_connect_to_database
from database.funcs_db import add_set_data_from_db
from google.functions import fetch_google_sheet_data
import asyncio
import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("core"))

async def get_area_warehouses():
    # url = "https://docs.google.com/spreadsheets/d/1m311sfWjhGUn1n3mi6G8dRT_j-UxHRIZDLvApqM3Zgg/edit?gid=0#gid=0"
    url = "https://docs.google.com/spreadsheets/d/1aBso_BgT-C7K16TGAWOxoQcw1NqclXgXXL9h15KmlOA/edit?gid=0#gid=0"

    base_data = fetch_google_sheet_data(
        url,
        0
    )
    headers = base_data[0]

    result = {}

    for row_idx, row in enumerate(base_data[1:], start=2):  # начинаем с 2, т.к. 1 - заголовок
        if not row or not row[0]:  # пропускаем пустые строки
            continue

        area = row[0].strip()  # название области
        warehouses_dict = {}

        # Проходим по каждому столбцу (кроме первого - области)
        for col_idx in range(1, len(headers)):
            warehouse_name = headers[col_idx].strip()

            # Проверяем, есть ли значение в этой ячейке
            if col_idx < len(row) and row[col_idx]:
                try:
                    value = int(row[col_idx])
                    warehouses_dict[warehouse_name] = value
                except (ValueError, TypeError) as e:
                    logger.warning(f"Не удалось преобразовать значение '{row[col_idx]}' "
                                   f"для области '{area}', склада '{warehouse_name}' "
                                   f"(строка {row_idx}, столбец {col_idx}): {e}")

        if warehouses_dict:  # только если есть хотя бы один склад
            result[area] = warehouses_dict
    conn = await async_connect_to_database()
    if not conn:
        logger.error("Ошибка подключения к БД")
        raise
    try:
        for area, warehouses in result.items():
            warehouses_json = json.dumps(warehouses)
            await conn.execute("""
                    INSERT INTO myapp_areawarehouses (area, warehouses)
                    VALUES ($1, $2)
                    ON CONFLICT (area)
                    DO UPDATE SET warehouses = EXCLUDED.warehouses;
                """, area, warehouses_json)
    except Exception as e:
        logger.error(f"Ошибка при добавлении/обновлении областей-складов. Ошибка: {e}")
        raise
    finally:
        await conn.close()

# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(get_area_warehouses())


