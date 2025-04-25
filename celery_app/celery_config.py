"""
Тут настраивается периодичность задач
"""

from celery import Celery
from celery.schedules import crontab
import logging


app = Celery(
    'pearhome',
    broker='redis://redis:6379/0',
    backend='redis://redis:6379/0',
    include=['tasks.google_wb_prices', 'tasks.google_our_prices']  # подключаем задачи
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# app.conf.timezone = 'Europe/Moscow'
app.conf.beat_schedule = {
    'update-prices-in-google-table': {
        'task': 'tasks.google_wb_prices.prices_table',
        'schedule': crontab(hour='7,10,14,18', minute=0),
    },
    'update-product-and-prices-db': {
        'task': 'tasks.google_our_prices.get_prices_and_products',
        'schedule': crontab(minute=33),
    }
}
