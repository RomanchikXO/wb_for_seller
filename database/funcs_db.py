from database.DataBase import async_connect_to_database
from celery_app.celery_config import logger
from typing import List, Optional, Dict, Any


async def get_data_from_db(
        table_name: str,
        columns: Optional[List[str]] = None,
        conditions: Optional[dict] = None
):
    """
    Получить данные из бд
    :param table_name: Название таблицы
    :param columns: Названия столбцов например ['id', 'name']
    :param conditions: Условия например {'id': '70', 'name': ['иван', 'олег']}
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
            conditions_str = (
                f"WHERE {key} = '{value}'" if isinstance(value, str)
                else f"WHERE {key} IN {tuple(value)}"
            )

    conn = await async_connect_to_database()

    if not conn:
        logger.warning(f"Ошибка подключения к БД в fetch_data__get_adv_id")
        return

    try:
        all_fields = await conn.fetch(f"SELECT {columns_str} FROM {table_name} {conditions_str}")
        return all_fields
    except Exception as e:
        logger.error(f"Ошибка получения данных из {table_name}. Error: {e}")
    finally:
        await conn.close()


async def add_set_data_from_db(
    table_name: str,
    data: Dict[str, Any],
    conflict_field: str = "id",  # по умолчанию обновляем по id
) -> None:
    """
    Добавить или обновить данные в таблице БД (UPSERT по conflict_field).

    :param table_name: Название таблицы
    :param data: Словарь с данными (ключ = имя поля)
    :param conflict_field: Поле, по которому проверяем конфликт (обычно id)
    :return: None

    Пример:
    await add_set_data_from_db(
    table_name="users",
    data={
        "id": 42,
        "name": "Alice",
        "email": "alice@example.com"
    }
)
    """
    if not data:
        logger.warning("Нет данных для вставки/обновления.")
        return

    conn = await async_connect_to_database()
    if not conn:
        logger.warning("Ошибка подключения к БД в add_set_data_from_db")
        return

    try:
        columns = list(data.keys())
        values = list(data.values())

        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        columns_str = ", ".join(columns)

        # Строим update-часть для ON CONFLICT
        update_str = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_field
        )

        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_field}) DO UPDATE SET {update_str}
        """

        await conn.execute(query, *values)
        logger.info(f"UPSERT в {table_name} прошел успешно")

    except Exception as e:
        logger.exception(f"Ошибка при UPSERT в {table_name}: {e}")
    finally:
        await conn.close()