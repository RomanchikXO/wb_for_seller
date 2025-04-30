import requests
from typing import List
import time
from loader import X_Mpstats_TOKEN
from celery.utils.log import get_task_logger
from datetime import datetime, timedelta

logger = get_task_logger("mpstat")

headers = {
    'X-Mpstats-TOKEN': X_Mpstats_TOKEN,
    'Content-Type': 'application/json',
}


def get_data(method: str, url: str, response_type="json", **kwargs):
    attempt, max_attemps = 0, 4
    while attempt <= max_attemps:
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            if response_type == "json":
                result = response.json()
            elif response_type == "text":
                result = response.text
            return result
        except Exception as e:
            attempt += 1
            logger.info(f"Can't get data, retry {attempt}. Url: {url}. Error: {e}")
            time.sleep(attempt * 2)


def get_revenue_mpstat(ids: List[str or int]):
    """
    Получить выручку по id товара из mpstat
    :param ids: id товаров (артикул)
    :return:
    """
    result = {}
    for id in ids:
        url = f"https://mpstats.io/api/wb/get/item/{id}/sales"
        response = get_data(
            method="get",
            url=url,
            response_type="json",
            headers=headers,
        )
        count_sales = 0
        total_price_sale = 0

        for record in response:
            count_sales += record["sales"]
            total_price_sale += (record["client_price"] * record["sales"])
        result[id] = total_price_sale

    return result


def get_full_mpstat(ids: List[str or int]):
    """
        Получить полную статистику по id товара из mpstat
        :param ids: id товаров (артикул)
        :return:
        """
    result = {}

    for id in ids:
        url = f"https://mpstats.io/api/wb/get/item/{id}/full"
        response = get_data(
            method="get",
            url=url,
            response_type="json",
            headers=headers,
        )

        result[id] = response

    return result


# get_full_mpstat([70497717,70497718, 70497720, 70497721, 70497722, 70498242, 70498243, 74512722, 74512723, 74512724, 75663661, 75663662, 75663663, 77455845])