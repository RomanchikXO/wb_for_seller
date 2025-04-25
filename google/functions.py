from typing import Union, List
import re
import aiohttp
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
from celery_app.celery_config import logger
from parsers.wildberies import wb_api
from database.funcs_db import get_data_from_db


current_directory = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.abspath(os.path.join(current_directory, "..", "credentials.json"))
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


colors = {
    "white": {"red": 1.0, "green": 1.0, "blue": 1.0},
    "light_green": {"red": 0.8, "green": 1.0, "blue": 0.8},
    "light_red": {"red": 1.0, "green": 0.8, "blue": 0.8},
    "dark_grey": {"red": 0.741, "green": 0.741, "blue": 0.741},
    "light_yellow": {"red": 1.0, "green": 1.0, "blue": 0.6},
}

def get_column_letter(n: int):
    """
    Вернуть название столбца по его индексу
    :param n:
    :return:
    """
    result = ''
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def get_ids_pages_table(url) -> List:
    response = []

    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=credentials)

    spreadsheet_id = url.split("/")[-2]  # ID_ТАБЛИЦЫ

    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet["sheets"]:
        response.append(sheet["properties"]["sheetId"])

    return response

def update_google_sheet_data(spreadsheet_url: str, sheet_identifier: int, data_range: str, values: List[list]):
    """
    Функция для обновления данных в Google Таблице.

    :param spreadsheet_url: URL таблицы (Google Spreadsheet URL)
    :param sheet_identifier: Идентификатор листа (индекс или имя листа)
    :param data_range: Диапазон данных в формате A1 (например, 'W4:AA34')
    :param values: Данные для обновления в виде списка списков
    :return: None
    """
    # Подключение к API
    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(credentials)

    # Открываем таблицу по URL
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Получаем лист (по индексу или имени)
    if isinstance(sheet_identifier, int):
        sheet = spreadsheet.get_worksheet(sheet_identifier)
    else:
        sheet = spreadsheet.worksheet(sheet_identifier)

    # Обновляем данные в указанном диапазоне
    try:
        sheet.update(data_range, values)
    except Exception as e:
        logger.error(f"Ошибка обновления данных в гугл таблице:{e}. Функция update_google_sheet_data")


def cleare_num(cell: str) -> Union[int, bool]:
    """
    привести строку в число либо вернуть False
    :param cell:
    :return:
    """
    try:
        result = int(str(cell).replace(" ", "").replace("\xa0", "").strip())
        return result
    except:
        return False


def update_google_sheet_data_with_format(
        spreadsheet_url: str,
        sheet_id: int,
        start_row: int,
        start_col: int,
        values: List[list]
):
    """
    Обновить данные в таблице с сохранением форматирования
    :param spreadsheet_url:
    :param sheet_id: номер листа (это число после '=' в ссылке)
    :param start_row: индекс строки (начиная с 0)
    :param start_col: индекс столбца (начиная с 0)
    :param values: Данные для обновления в виде списка списков
    :return:
    """

    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    spreadsheet_id = spreadsheet_url.split("/")[-2] #ID_ТАБЛИЦЫ

    rows = []
    #ниже создаем массив с индексами всех строк с размерами
    indexes_row_sizes = [ind for ind, _data in enumerate(values) if re.match(r'^\d+х\d+$', _data[0])]
    indexes_row_sizes.append(len(values))
    # Преобразуем values → rows with userEnteredValue
    try:
        for ind_1, row in enumerate(values): # тут итерация по строкам
            row_data = {"values": []}
            numbers_grey = [i for i in range(5, 100, 4)]
            if ind_1 == 0:
                for ind_2, cell in enumerate(row): # тут итерация по клеткам первой строки
                    try:
                        int(cell.replace(" ", "").replace("\xa0", "").strip())
                        letter = get_column_letter(ind_2+1)
                        formula_list = [f"{letter}{i+1}" for i in indexes_row_sizes[:-1]]
                        formula = "+".join(formula_list)
                        row_data["values"].append({"userEnteredValue": {"formulaValue": f"=SUM({formula})"}})
                    except:
                        if ind_2 in numbers_grey:
                            row_data["values"].append(
                                {
                                    "userEnteredValue": {"stringValue": str(cell)},
                                    "userEnteredFormat": {"backgroundColor": colors["dark_grey"]}
                                }
                            )
                        else:
                            row_data["values"].append({"userEnteredValue": {"stringValue": str(cell)}})
            elif re.match(r'^\d+х\d+$', row[0]): # серые строки с размерами
                for ind_2, cell in enumerate(row):
                    if cell and not re.match(r'^\d+х\d+$', cell):
                        row_data["values"].append(
                            {
                                "userEnteredValue": {"formulaValue": f"=SUM({get_column_letter(ind_2+1)}{ind_1+2}:{get_column_letter(ind_2+1)}{indexes_row_sizes[indexes_row_sizes.index(ind_1)+1]})"},
                                "userEnteredFormat": {"backgroundColor": colors["dark_grey"]}
                            }
                        )
                    else:
                        row_data["values"].append(
                            {
                                "userEnteredValue": {"stringValue": str(cell)},
                                "userEnteredFormat": {"backgroundColor": colors["dark_grey"]}
                            }
                        )
            else:
                for ind_3, cell in enumerate(row): # итерация по клеткам с ценами и доходом
                    if ind_3 == 1:
                        count_column = len(values[0]) // 4
                        list_column_letter = [f"{get_column_letter(i * 4)}{ind_1+1}" for i in range(1, count_column+1)]
                        formula = "+".join(list_column_letter)
                        row_data["values"].append({"userEnteredValue": {"formulaValue": f"=SUM({formula})"}})
                    else:
                        try:
                            numberValue = cleare_num(cell)
                            if not numberValue: raise

                            profits = [cleare_num(cell) for cell in row[3::4] if not isinstance(cell, str)]
                            max_profit = max(profits)
                            min_profit = min(profits)

                            if max_profit == min_profit:
                                backgroundColor = colors["white"]
                            elif numberValue == max_profit:
                                backgroundColor = colors["light_green"]
                            elif numberValue == min_profit:
                                backgroundColor = colors["light_red"]
                            else:
                                backgroundColor = colors["white"]

                            row_data["values"].append(
                                {
                                    "userEnteredValue": {
                                        "numberValue": numberValue,
                                    },
                                    "userEnteredFormat": {
                                        "backgroundColor": backgroundColor,
                                    }

                                }
                            )
                        except:
                            if ind_3 in numbers_grey:
                                row_data["values"].append(
                                    {
                                        "userEnteredValue": {"stringValue": str(cell)},
                                        "userEnteredFormat": {"backgroundColor": colors["dark_grey"]}
                                    }
                                )
                            else:
                                row_data["values"].append({"userEnteredValue": {"stringValue": str(cell)}})
            rows.append(row_data)
    except Exception as e:
        logger.error(f"Ошибка обработки данных для гугл таблицы: {e}. Функция: update_google_sheet_data_with_format")

    # Формируем запрос
    request_body = {
        "requests": [
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": start_col,
                        "endRowIndex":len(values),
                        "endColumnIndex": len(values[0])
                    },
                    "rows": rows,
                    "fields": "userEnteredValue,userEnteredFormat.backgroundColor"  # ВАЖНО: данные, и цвета
                }
            }
        ]
    }

    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()
    except Exception as e:
        logger.error(f"Ошибка обновления данных в гугл таблице: {e}. Функция: update_google_sheet_data_with_format")

def add_nmids_to_google_table(data: list, range: str, index=0) -> None:
    # Задайте параметры таблицы
    SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1bA0qD8XAVMFqZt5pOlUqrG7_rBYQ-JVngcd0mxuncNo/edit?gid=1401527408#gid=1401527408"
    SHEET_IDENTIFIER = 6  # Индекс листа (начинается с 0) или его имя (например, "Лист1")
    DATA_RANGE = range  # Пример:"A1:C2" Укажите диапазон или оставьте None для всего листа

    try:
        res = update_google_sheet_data(SPREADSHEET_URL, SHEET_IDENTIFIER, DATA_RANGE, data)
    except Exception as e:
        logger.error(f"Ошибка в add_nmids_to_google_table. Error: {e}")


def update_google_prices_data_with_format(
        spreadsheet_url: str,
        sheet_id: int,
        start_row: int,
        start_col: int,
        values: List[list],
        **kwargs
):
    """
    Обновить данные в таблице с сохранением форматирования
    :param spreadsheet_url: url таблицы
    :param sheet_id: номер листа (это число после '=' в ссылке)
    :param start_row: индекс строки (начиная с 0)
    :param start_col: индекс столбца (начиная с 0)
    :param values: Данные для обновления в виде списка списков
    :return:
    """
    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    spreadsheet_id = spreadsheet_url.split("/")[-2]  # ID_ТАБЛИЦЫ
    rows = []

    try:
        for ind_1, row in enumerate(values): # итерация по строкам
            row_data = {"values": []}
            for ind_2, cell in enumerate(row):
                if ind_1 == 0:
                    if ind_2 == 8:
                        row_data["values"].append({
                            "userEnteredValue": {"stringValue": str(cell)},
                            "userEnteredFormat": {
                                "textFormat": {"bold": True},
                                "backgroundColor": colors["light_yellow"]
                            }
                        })
                    else:
                        if ind_2 == (len(row)-1):
                            row_data["values"].append({
                                "userEnteredValue": {"stringValue": f"{kwargs['discount']}%"}
                            })
                        else:
                            row_data["values"].append({
                                "userEnteredValue": {"stringValue": str(cell)},
                                "userEnteredFormat": {
                                    "textFormat": {
                                        "bold": True
                                    }
                                }
                            })
                else:
                    if ind_2 == 8:
                        row_data["values"].append({
                            "userEnteredValue": {"numberValue": cleare_num(cell)},
                            "userEnteredFormat": {
                                "backgroundColor": colors["light_yellow"]
                            }
                        })
                    try:
                        numberValue = cleare_num(cell)
                        row_data["values"].append({
                            "userEnteredValue": {"numberValue": numberValue},
                        })
                    except:
                        row_data["values"].append({
                            "userEnteredValue": {"stringValue": cell},
                        })
            rows.append(row_data)
    except Exception as e:
        logger.error(f"Ошибка обработки данных для гугл таблицы: {e}. Функция: update_google_sheet_data_with_format")


    # Формируем запрос
    request_body = {
        "requests": [
            {
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": start_col,
                        "endRowIndex": len(values),
                        "endColumnIndex": len(values[0])
                    },
                    "rows": rows,
                    "fields": "userEnteredValue,userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.bold"  # ВАЖНО: данные, цвета, шрифт
                }
            }
        ]
    }

    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()
    except Exception as e:
        logger.error(f"Ошибка обновления данных в гугл таблице: {e}. Функция: update_google_prices_data_with_format")


def fetch_google_sheet_data(spreadsheet_url, sheet_identifier: Union[int, str, None], data_range=None):
    """
    Функция для извлечения данных из Google Таблицы.

    :param spreadsheet_url: URL таблицы (Google Spreadsheet URL)
    :param sheet_identifier: Идентификатор листа (индекс начиная с 0 или имя листа)
    :param data_range: Диапазон данных в формате A1 (например, 'W4:AA34') или None для всего листа
    :return: Данные как список списков
    """
    # Подключение к API

    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(credentials)

    # Открываем таблицу по URL
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Получаем лист (по индексу или имени)
    if isinstance(sheet_identifier, int):
        sheet = spreadsheet.get_worksheet(sheet_identifier)
    elif isinstance(sheet_identifier, str):
        sheet = spreadsheet.worksheet(sheet_identifier)
    else:
        sheets = spreadsheet.worksheets()
        return sheets

    # Извлекаем данные
    if data_range:
        data = sheet.get(data_range)  # Данные из указанного диапазона
    else:
        data = sheet.get_all_values()  # Все данные с листа

    return data


