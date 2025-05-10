from database.DataBase import connect_to_database
import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("bot"))

def get_status(tg_id: int) -> str:
    query = "SELECT tg_status from myapp_customuser where tg_id = %s"
    status = ""
    conn = connect_to_database()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (tg_id,))
            response = cursor.fetchall()
            if response:
                status = response[0][0] if response else None
    except Exception as e:
        logging.error(f"Ошибка получения статуса пользователя: {e}")
    finally:
        conn.close()

    return status


def set_status(status: str, tg_id: int):
    query = "UPDATE myapp_customuser SET tg_status = %s where tg_id = %s"
    conn = connect_to_database()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (status, tg_id,))
            conn.commit()
    except Exception as e:
        logging.error(f"Ошибка обновления статуса пользователя: {e}")
    finally:
        conn.close()

