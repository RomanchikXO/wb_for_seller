import logging
from database.DataBase import connect_to_database
from google.functions import get_time_msk


class DBLogHandler(logging.Handler):
    def emit(self, record):
        conn = connect_to_database()  # Синхронное подключение к базе данных
        if conn is None:
            print("Failed to connect to the database")
            return

        try:
            # Формируем сообщение для вставки
            formatted_message = self.format(record)
            current_time = get_time_msk()

            # Используем SQL для вставки данных в таблицу
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO myapp_celerylog (timestamp, level, message)
                    VALUES (%s, %s, %s)
                    """,
                    (current_time, record.levelname, formatted_message)
                )
            conn.commit()  # Сохраняем изменения
        except Exception as e:
            print(f"Error saving log to DB: {e}")
        finally:
            conn.close()


