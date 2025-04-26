from typing import List
from parsers.wildberies import parse
from google.functions import fetch_google_sheet_data, update_google_sheet_data_with_format, get_column_letter, get_ids_pages_table
from mpstat import get_revenue_mpstat
from celery.utils.log import get_task_logger

logger = get_task_logger("core")
url_prices = "https://docs.google.com/spreadsheets/d/1EhjutxGw8kHlW1I3jbdgD-UMA5sE20aajMO865RzrlA/edit?gid=1101267699#gid=1101267699"


def get_count_pages(url: str) -> int:
    """
    Получить кол-во листов
    :param url: url таблы
    :return: кол-во листов в таблице
    """
    try:
        data = fetch_google_sheet_data(url, sheet_identifier=None)
        return len(data)
    except Exception as e:
        logger.error(f"Ошибка: {e}. Функция get_count_pages. Параметры: {url}")


def get_data_lists(url: str) -> List[list]:
    """
    получть информацию из всех листов таблицы
    :param url:
    :return: массив с информацуией на каждом листе
    """
    count_pages = get_count_pages(url)

    result = []

    for page in range(count_pages):
        data = fetch_google_sheet_data(url, sheet_identifier=page)
        result.append(data)

    return result


def process_data(url: str) -> None:
    """
    основная функция для работы с таблицей с ценами
    :param url:
    :return:
    """
    data = get_data_lists(url)

    for index_page, page in enumerate(data): #итерация по листам
        list_data = {}
        for i_index, line in enumerate(page): #проходим по строке на листе
            list_data[i_index] = [i for i in line[2::4]]

        for i_index, nmIds in list_data.items(): #i_index это номер строки -1
            for nmId in nmIds:
                try:
                    nmId = int(nmId)
                    price = parse([nmId])[0]
                    revenue = get_revenue_mpstat([nmId])[nmId]
                    current_index = data[index_page][i_index].index(str(nmId))
                    data[index_page][i_index][current_index+1] = revenue
                    data[index_page][i_index][current_index + 2] = price
                except:
                    pass

    numbers_pages = get_ids_pages_table(url)
    for index, page in enumerate(data):
        sheet_id = numbers_pages[index]
        update_google_sheet_data_with_format(url, sheet_id, 0, 0, page)

