import psycopg2
import asyncpg
from loader import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("database"))


DATABASE_CONFIG = {
    'dbname': POSTGRES_DB,
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'host': 'db',
    'port': 5432,
}


def connect_to_database():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None


async def async_connect_to_database():
    """
    Асинхронное подключение к базе данных с использованием пула соединений.
    """
    try:
        return await asyncpg.create_pool(
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
            database=DATABASE_CONFIG['dbname'],
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port']
        )
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None


def close_connection(conn):
    if conn:
        conn.close()

