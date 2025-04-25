from database.DataBase import async_connect_to_database
from celery_app.celery_config import logger
from typing import List, Optional, Dict, Any


async def get_data_from_db(
        table_name: str,
        columns: Optional[List[str]] = None,
        conditions: Optional[dict] = None,
        additional_conditions: Optional[str] = None
):
    """
    Получить данные из бд
    :param table_name: Название таблицы
    :param columns: Названия столбцов например ['id', 'name']
    :param conditions: Условия например {'id': '70', 'name': ['иван', 'олег']}
    :param additional_conditions: Дополнительные условия для JOIN или других фильтров

    :return:
    """
    if columns is None:
        columns_str = '*'
    else:
        columns_str = ", ".join(columns) if len(columns) > 1 else columns[0]

    if conditions is None:
        conditions_str = ''
    else:
        if len(conditions) > 1:
            conditions_str = "WHERE " + " AND ".join(
                f"{key} = '{value}'" if isinstance(value, str) else f"{key} IN {tuple(value)}"
                for key, value in conditions.items()
            )
        else:
            key, value = next(iter(conditions.items()))
            if isinstance(value, str):
                conditions_str = f"WHERE {key} = '{value}'"
            elif isinstance(value, int):
                conditions_str = f"WHERE {key} = {value}"
            else:
                f"WHERE {key} IN {tuple(value)}"

    if additional_conditions:
        if conditions is None:
            conditions_str = f" WHERE {additional_conditions}"
        else:
            conditions_str += f" AND {additional_conditions}"

    conn = await async_connect_to_database()

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        return

    try:
        request = f"SELECT {columns_str} FROM {table_name} {conditions_str}"
        all_fields = await conn.fetch(request)
        return all_fields
    except Exception as e:
        logger.error(f"Ошибка получения данных из {table_name}. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


async def add_set_data_from_db(
    conn,
    table_name: str,
    data: Dict[str, Any],
    conflict_fields: list = None,
) -> None:
    """
    Добавить или обновить данные в таблице БД (UPSERT по conflict_field).

    :param table_name: Название таблицы
    :param data: Словарь с данными (ключ = имя поля)
    :param conflict_fields: Список полей, по которым проверяем конфликт (например, ["nmid", "lk_id"]). Если не передан, используется ["id"]
    :return: None
    """
    need_close = False
    if not data:
        logger.warning("Нет данных для вставки/обновления.")
        return

    # Если conflict_fields не передан, используем ["id"] как значение по умолчанию
    if not conflict_fields:
        conflict_fields = ["id"]

    if not conn:
        need_close = True
        conn = await async_connect_to_database()
        if not conn:
            logger.warning("Ошибка подключения к БД в add_set_data_from_db")
            return

    try:
        columns = list(data.keys())
        values = list(data.values())

        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        columns_str = ", ".join(columns)

        # Строим строку для конфликтующих полей
        conflict_columns_str = ", ".join(conflict_fields)

        # Строим update-часть для ON CONFLICT
        update_str = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_fields
        )

        # Формируем запрос
        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_columns_str}) DO UPDATE SET {update_str}
        """

        # Выполняем запрос
        await conn.execute(query, *values)
        logger.info(f"UPSERT в {table_name} прошел успешно")

    except Exception as e:
        logger.exception(f"Ошибка при UPSERT в {table_name}: {e}")
    finally:
        if need_close:
            await conn.close()
