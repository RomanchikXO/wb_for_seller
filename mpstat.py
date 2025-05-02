import requests
from typing import List
import time
from loader import X_Mpstats_TOKEN

import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("mpstat"))

headers = {
    'X-Mpstats-TOKEN': X_Mpstats_TOKEN,
    'Content-Type': 'application/json',
}


def get_data(method: str, url: str, response_type="json", **kwargs):
    attempt, max_attemps = 0, 4
    while attempt <= max_attemps:
        try:
            response = requests.request(method, url, **kwargs)
            if response_type == "json":
                result = response.json()
                if isinstance(result, dict) and result.get("message") == "SKU не найден":
                    return result
            elif response_type == "text":
                result = response.text
            response.raise_for_status()
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
        if response.get("message") == "SKU не найден":
            continue
        result[id] = response

    return result