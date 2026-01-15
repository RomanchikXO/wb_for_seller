from typing import List, Dict
from myapp.models import nmids as nmids_db
from datetime import datetime, timedelta
import logging
from context_logger import ContextLogger
from asgiref.sync import async_to_sync
from database.DataBase import connect_to_database, async_connect_to_database
import asyncio
from collections import defaultdict
import re
from django.core.paginator import Paginator
from django.shortcuts import render


logger = ContextLogger(logging.getLogger("parsers"))

async def get_all_orders(pool, nmid_query_filter, period):
    # итоговая структура: { nmid: { warehouse: count } }
    all_orders = {}

    # заказы для каждого склада
    sql_query = f"""
            WITH region_warehouse_min AS (
                SELECT
                    area,
                    (SELECT key
                     FROM jsonb_each_text(warehouses)
                     ORDER BY (value)::int
                     LIMIT 1) AS min_warehouse
                FROM myapp_areawarehouses
            ),
            orders_with_warehouse AS (
                SELECT
                    o.nmid,
                    rwm.min_warehouse
                FROM myapp_orders o
                JOIN region_warehouse_min rwm
                    ON o.regionname = rwm.area
                WHERE
                    o.date >= $1
                    AND {nmid_query_filter}
            )
            SELECT
                nmid,
                min_warehouse AS warehouse_with_min_value,
                COUNT(*) AS order_count
            FROM orders_with_warehouse
            GROUP BY nmid, min_warehouse
            ORDER BY order_count DESC;
        """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql_query, period)

            result = defaultdict(dict)
            for row in rows:
                nmid = row["nmid"]
                warehouse = row["warehouse_with_min_value"]
                count = row["order_count"]
                result[nmid][warehouse] = count

            all_orders = dict(result)

    except Exception as e:
        logger.exception(f"Сбой при выполнении podsort_view для заказов. Error: {e}")

    return all_orders


async def get_articles(pool, nmid_query):
    # артикул, id, ткань и цвет
    sql_query = f"""
            SELECT 
                nmid,
                id,
                (
                    SELECT (elem->'value')->>0 AS value
                    FROM jsonb_array_elements(characteristics) AS elem
                    WHERE (elem->>'id')::int = 12
                    LIMIT 1
                ) AS cloth,
                (
                    SELECT (elem->'value')->>0 AS value
                    FROM jsonb_array_elements(characteristics) AS elem
                    WHERE (elem->>'id')::int = 14177449
                    LIMIT 1
                ) AS i_color,
                vendorcode,
                (
                    SELECT COALESCE(json_agg(t.tag), '[]'::json)
                    FROM myapp_tags t
                    WHERE t.id = ANY(
                        SELECT jsonb_array_elements_text(tag_ids)::int
                        FROM myapp_nmids n2
                        WHERE n2.id = myapp_nmids.id
                    )
                ) AS tag_ids
            FROM myapp_nmids
            WHERE {nmid_query}
        """

    articles = {}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql_query)
            articles = {
                row['nmid']: {
                    "id": row['id'],
                    "cloth": row['cloth'],
                    "i_color": row['i_color'],
                    "vendorcode": row['vendorcode'],
                    "tag_ids": row['tag_ids']
                }
                for row in rows
            }
    except Exception as e:
        logger.exception(
            f"Сбой при выполнении podsort_view при получении артикул, id, ткань и цвет. Error: {e}"
        )

    return articles


async def fetch_all_data(nmid_query_filter, period, name_column_available, nmid_query):
    """Главная функция для параллельных запросов"""
    pool = await async_connect_to_database()
    if pool is None:
        raise Exception("Ошибка подключения к БД")

    try:
        results = await asyncio.gather(
            get_warh(pool),
            get_all_orders(pool, nmid_query_filter, period),
            get_warh_stock(pool, name_column_available, nmid_query),
            get_articles(pool, nmid_query)
        )
    except Exception as e:
        raise Exception(f"Ошибка при параллельном получении данных: {e}")
    finally:
        if pool:
            await pool.close()

    return results


def filter_by(items: dict, filter_param: str):
    try:
        filter_items = dict(
            filter(lambda item: item[1]["ABC"] == filter_param, items.items())
        )
    except Exception as e:
        raise Exception(f"Ошибка фильтрации: {e}")
    return filter_items


def sorted_by(items: dict, sort_by: str, descending: bool = False) -> dict:
    """
    Сортирует словарь items по значению ключа sort_by во вложенных словарях.

    :param items: словарь, где ключ — артикул, значение — словарь с ключом sort_by
    :param descending: если True — сортировка по убыванию, иначе — по возрастанию
    :return: новый отсортированный словарь
    """
    try:
        sorted_items = dict(
            sorted(
                items.items(),
                key=lambda item: item[1].get(sort_by, 0),
                reverse=descending
            )
        )
    except Exception as e:
        raise Exception(f"Ошибка сортировки: {e}")
    return sorted_items


def get_filter_by_articles(current_ids, clothes: bool = False, sizes: bool = False, size_color: bool = False, colors: bool = False):

    sql_query = f"""
        WITH base AS (
          SELECT
            m.nmid,
            рисунок.value AS main_group,
            цвет.value AS color
          FROM myapp_nmids m
          LEFT JOIN LATERAL (
            SELECT (item->'value')->>0 AS value
            FROM jsonb_array_elements(m.characteristics) AS item
            WHERE (item->>'id')::int = 12
            LIMIT 1
          ) AS рисунок ON TRUE
          LEFT JOIN LATERAL (
            SELECT (item->'value')->>0 AS value
            FROM jsonb_array_elements(m.characteristics) AS item
            WHERE (item->>'id')::int = 14177449
            LIMIT 1
          ) AS цвет ON TRUE
          WHERE рисунок.value IS NOT NULL AND цвет.value IS NOT NULL AND m.nmid IN ({', '.join(map(str, current_ids))})
        )
        SELECT jsonb_object_agg(main_group, colors) AS result
        FROM (
          SELECT
            main_group,
            jsonb_object_agg(color, nmid) AS colors
          FROM (
            SELECT
              main_group,
              color,
              jsonb_agg(nmid) AS nmid
            FROM base
            GROUP BY main_group, color
          ) AS grouped
          GROUP BY main_group
        ) AS final;
    """

    # Выполнение SQL запроса и получение данных
    try:
        conn = connect_to_database()
        with conn.cursor() as cursor:
            cursor.execute(sql_query, )
            rows = cursor.fetchall()
    except Exception as e:
        logging.error(f"Ошибка при запросе в get_filter_by_articles без условия {e}")

    columns = [desc[0] for desc in cursor.description]
    dict_rows = [dict(zip(columns, row)) for row in rows]
    data = dict_rows[0]["result"]

    response = {}

    if colors:
        sql_query = f"""
            SELECT json_agg(json_build_object(color_key, nmid_list)) AS result
            FROM (
                SELECT
                    color_key,
                    array_agg(nmid) AS nmid_list
                FROM (
                    SELECT
                        p.nmid,
                        (
                            SELECT (elem->'value')->>0
                            FROM jsonb_array_elements(p.characteristics) AS elem
                            WHERE (elem->>'id')::int = 14177449
                            LIMIT 1
                        ) AS color_key
                    FROM myapp_nmids p
                    WHERE p.nmid IN ({', '.join(map(str, current_ids))})
                ) AS extracted
                WHERE color_key IS NOT NULL AND color_key <> ''
                GROUP BY color_key
            ) AS grouped;
        """

        try:
            conn = connect_to_database()
            with conn.cursor() as cursor:
                cursor.execute(sql_query, )
                rows = cursor.fetchall()
        except Exception as e:
            logging.error(f"Ошибка при запросе в get_filter_by_articles при условии colors {e}")

        columns = [desc[0] for desc in cursor.description]
        dict_rows = [dict(zip(columns, row)) for row in rows]
        colors = dict_rows[0]["result"]

        changed_colors = [
            {
                'tail': key,
                "nmids": value
            }
            for dictionary in colors
            for key, value in dictionary.items()
        ]
        response["colors"] = sorted(changed_colors, key=lambda x: x['tail'])
    if clothes:
        cloth = sorted(
            [
                {
                    'tail': tail,
                    'nmids': sum(colors.values(), [])  # объединяем все списки артикулов в один
                }
                for tail, colors in data.items()
            ],
            key=lambda x: x['tail'].lower()
        )
        response["cloth"] = cloth
    if sizes:
        sql_query = f"""
            SELECT json_agg(json_build_object(lower_code, nmid_list)) AS result
            FROM (
                SELECT
                    lower_code,
                    array_agg(nmid) AS nmid_list
                FROM (
                    SELECT
                        nmid,
                        CASE
                            WHEN lower(vendorcode) LIKE '%11ww%' THEN '3240'
                            WHEN lower(vendorcode) LIKE '%22ww%' THEN '3270'
                            WHEN lower(vendorcode) LIKE '%33ww%' THEN '3250'
                            WHEN lower(vendorcode) LIKE '%44ww%' THEN '3260'
                            WHEN lower(vendorcode) LIKE '%55ww%' THEN '4240'
                            WHEN lower(vendorcode) LIKE '%66ww%' THEN '4250'
                            WHEN lower(vendorcode) LIKE '%77ww%' THEN '4260'
                            WHEN lower(vendorcode) LIKE '%88ww%' THEN '4270'
                            WHEN lower(vendorcode) LIKE '%2240%' THEN '2240'
                            WHEN lower(vendorcode) LIKE '%2250%' THEN '2250'
                            WHEN lower(vendorcode) LIKE '%2260%' THEN '2260'
                            WHEN lower(vendorcode) LIKE '%2270%' THEN '2270'
                            WHEN lower(vendorcode) LIKE '%3240%' THEN '3240'
                            WHEN lower(vendorcode) LIKE '%3250%' THEN '3250'
                            WHEN lower(vendorcode) LIKE '%3260%' THEN '3260'
                            WHEN lower(vendorcode) LIKE '%3270%' THEN '3270'
                            WHEN lower(vendorcode) LIKE '%4240%' THEN '4240'
                            WHEN lower(vendorcode) LIKE '%4250%' THEN '4250'
                            WHEN lower(vendorcode) LIKE '%4260%' THEN '4260'
                            WHEN lower(vendorcode) LIKE '%4270%' THEN '4270'
                            WHEN lower(vendorcode) LIKE '%5240%' THEN '5240'
                            WHEN lower(vendorcode) LIKE '%5250%' THEN '5250'
                            WHEN lower(vendorcode) LIKE '%5260%' THEN '5260'
                            WHEN lower(vendorcode) LIKE '%5270%' THEN '5270'
                            WHEN lower(vendorcode) LIKE '%6240%' THEN '6240'
                            WHEN lower(vendorcode) LIKE '%6250%' THEN '6250'
                            WHEN lower(vendorcode) LIKE '%6260%' THEN '6260'
                            WHEN lower(vendorcode) LIKE '%6270%' THEN '6270'
                            ELSE NULL
                        END AS lower_code
                    FROM myapp_nmids
                    WHERE nmid IN ({', '.join(map(str, current_ids))})
                ) AS filtered
                WHERE lower_code IS NOT NULL
                GROUP BY lower_code
            ) AS grouped;
        """
        try:
            conn = connect_to_database()
            with conn.cursor() as cursor:
                cursor.execute(sql_query, )
                rows = cursor.fetchall()
        except Exception as e:
            logging.error(f"Ошибка при запросе в get_filter_by_articles при условии sizes {e}")

        columns = [desc[0] for desc in cursor.description]
        dict_rows = [dict(zip(columns, row)) for row in rows]
        sizes = dict_rows[0]["result"]

        changed_sizes = [
            {
                'tail': key,
                "nmids": value
            }
            for dictionary in sizes
            for key, value in dictionary.items()
        ]
        response["sizes"] = sorted(changed_sizes, key=lambda x: int(x['tail']))
    if size_color:
        response["size_color"] = sorted(
            [
                {
                    'tail': f"{tail}-{color}",
                    'nmids': ids
                }
                for tail, colors in data.items() for color, ids in colors.items()
            ],
            key=lambda x: x['tail'].lower()
        )
    return response


def abc_classification(data: dict):
    # Шаг 1: Сортируем по количеству заказов (убывание)
    try:
        sorted_items = sorted(data.items(), key=lambda x: x[1]["orders"], reverse=True)

        # Шаг 2: Присваиваем категорию
        for i, (art, info) in enumerate(sorted_items):
            vendorcode = info["vendorcode"].lower()
            if ("тв" in vendorcode or "мрк" in vendorcode or "тм2270" in vendorcode or "тм3270" in vendorcode or "тм4270" in vendorcode
                    or "мр3250" in vendorcode or "мр3260" in vendorcode or "мр3270" in vendorcode or "мр4270" in vendorcode
                    or "бл3250" in vendorcode or "бл3260" in vendorcode or "бл3270" in vendorcode or "бл4270" in vendorcode):
                info["ABC"] = "Новинки"
            elif ("240" in vendorcode or "250" in vendorcode or "11ww" in vendorcode or "33ww" in vendorcode
                    or "55ww" in vendorcode or "66ww" in vendorcode):
                info["ABC"] = "A"
            elif ("260" in vendorcode or "4270" in vendorcode or "44ww" in vendorcode or "77ww" in vendorcode
                  or "88ww" in vendorcode):
                info["ABC"] = "B"
            elif "3270" in vendorcode or "2270" in vendorcode or "22ww" in vendorcode:
                info["ABC"] = "C"
    except Exception as e:
        raise Exception(f"Ошибка abc классификатора: {e}")
    return dict(sorted_items)


def get_group_nmids(nmids):
    tail_groups = defaultdict(list)

    for item in nmids:
        vendorcode = item['vendorcode']
        nmid = item['nmid']

        # Проверяем, начинается ли строка с букв и заканчивается цифрами
        if re.match(r'^[A-Za-zА-Яа-я\W_]+[0-9]+$', vendorcode):
            # Весь vendorcode целиком
            tail = vendorcode
        else:
            # Убираем все цифры из строки
            tail = re.sub(r'\d+', '', vendorcode)

        tail_groups[tail].append(nmid)

    # Преобразуем в список для передачи в шаблон
    # Сортировка по алфавиту по ключу 'tail'
    tail_filter_options = sorted(
        [{'tail': tail, 'nmids': ids} for tail, ids in tail_groups.items()],
        key=lambda x: x['tail'].lower()  # регистронезависимо
    )

    return tail_filter_options


def get_current_nmids()-> List[int]:
    """
    получаем массив артикулов которые нужны селлеру
    Returns:

    """
    try:
        active_nmids = nmids_db.objects.filter(is_active=True).values_list('nmid', flat=True)
        active_nmids_list = list(active_nmids)
    except Exception as e:
        raise Exception(f"Ошибка в get_current_nmids: {e}")
    return active_nmids_list


def business_logic_podsort(
        warehouse_filter: List, parametrs,
        turnover_change, all_filters, request = None
):
    export_mode = parametrs['export_mode']
    current_ids = get_current_nmids()

    if all_filters:
        all_filters = list(set.intersection(*all_filters))
        all_current_ids = list(set(map(str, current_ids)) & set(all_filters))
        nmid_query = f"nmid IN ({', '.join(map(str, all_current_ids))})"
        nmid_query_filter = f"o.nmid IN ({', '.join(map(str, all_current_ids))})"
    else:
        nmid_query = f"nmid IN ({', '.join(map(str, current_ids))})"
        nmid_query_filter = f"nmid IN ({', '.join(map(str, current_ids))})"

    if nmid_query_filter == "o.nmid IN ()": nmid_query_filter = "o.nmid IN (0)"
    if nmid_query == "nmid IN ()": nmid_query = "nmid IN (0)"

    now_msk = datetime.now() + timedelta(hours=3)
    yesterday_end = now_msk.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    tree_days_ago = yesterday_end - timedelta(days=3)
    seven_days_ago = yesterday_end - timedelta(days=7)
    two_weeks_ago = yesterday_end - timedelta(weeks=2)
    thirty_days_ago = yesterday_end - timedelta(days=30)
    period_ord = parametrs["period_ord"]
    if period_ord == 3:
        period = tree_days_ago
        name_column_available = "days_in_stock_last_3"
    elif period_ord == 7:
        period = seven_days_ago
        name_column_available = "days_in_stock_last_7"
    elif period_ord == 14:
        period = two_weeks_ago
        name_column_available = "days_in_stock_last_14"
    elif period_ord == 30:
        period = thirty_days_ago
        name_column_available = "days_in_stock_last_30"

    try:
        warehouses, all_orders, warh_stock, articles = async_to_sync(fetch_all_data)(
            nmid_query_filter,
            period,
            name_column_available,
            nmid_query
        )
    except Exception as e:
        logger.exception(f"Ошибка при параллельном получении данных: {e}")

    if warehouse_filter:
        orders_with_filter = get_orders_with_filter(nmid_query_filter, warehouse_filter, period)
    # Если складов не было возвращаем сразу результат
    try:
        if not warehouse_filter:
            response = _podsort_view(None, articles, warh_stock, period_ord,
                                     all_orders, warehouses, current_ids, parametrs, False, export_mode)

            if export_mode: return response

            return render(
                request,
                "podsort.html",
                response
            )
    except Exception as e:
        logger.error(f"Ошибка запроса без фильтра складов {e}")
        raise

    # Если склады есть
    try:
        # с фильтрами
        full_data = _podsort_view(
            orders_with_filter, articles, warh_stock, period_ord, all_orders, warehouses, current_ids,
            parametrs, True, export_mode
        )

        sum_short_orders = {}  # все заказы с фильтрами
        try:
            for art, warh in orders_with_filter.items():
                sum_short_orders[art] = sum(list(warh.values()))
        except Exception as e:
            raise Exception(f"Ошибка при подсчете total_short_rec_del {e}")

        for i in full_data["items"].object_list:
            if subitems := i.get("subitems"):

                # сумма остатков НЕ выбранных складов для артикула
                sum_stock_without_check_warh = sum(
                    [subitem["stock"] for subitem in subitems if subitem["warehouse"] not in warehouse_filter])

                # Собираем коэффициены на которые надо будет домножать остатки не выбр складов и суммируем
                stock_koefs = [
                    (
                        sum_stock_without_check_warh / subitem["order_for_change_war"]
                        if subitem["order_for_change_war"]
                        else 1
                    )
                    for subitem in subitems
                    if subitem["warehouse"] in warehouse_filter
                ]
                stock_koefs = sum(stock_koefs)

                # множитель на который надо будет домножать отдельные коэфы выбранных складов
                x = sum_stock_without_check_warh / stock_koefs if stock_koefs else 1

                sum_rec_del = 0
                _index = 0
                for index, subitem in enumerate(subitems):
                    if not subitem["warehouse"] in warehouse_filter:
                        # пропускаем не выбранные склады ибо нахер не нужныв
                        continue
                    _index = index
                    # высчитывает коэф на который надо будет домножать остатки не выбр складов
                    # по заказам выбранных складов
                    stock_koef = sum_stock_without_check_warh / subitem["order_for_change_war"] if subitem["order_for_change_war"] else 1

                    # распределяем остатки не выбранных складов на выбранные (на фронт они не передаются)
                    new_stock = stock_koef * x + subitem["stock"]

                    # высчитывыаем поставку на основе новых остатков
                    subitem["rec_delivery"] = round(
                        subitem["order_for_change_war"] / period_ord * turnover_change - new_stock)
                    sum_rec_del += subitem["rec_delivery"]

                    # здесь считаем новую оборачиваемость
                    subitem["turnover"] = new_stock / (subitem["order_for_change_war"] / period_ord) if subitem["order_for_change_war"] else new_stock
                if sum_rec_del and period_ord == 30 and turnover_change == 30:
                    difference = i["orders"] - (sum_rec_del + i["stock"])
                    if difference != 0:
                        subitems[_index]["rec_delivery"] += difference

        if export_mode: return full_data
        return render(
            request,
            "podsort.html",
            full_data
        )
    except Exception as e:
        logger.error(f"Какая то ошибка {e}")


async def get_warh(pool):
    sql_query = """
        SELECT DISTINCT jsonb_object_keys(warehouses) AS warehouse
        FROM myapp_areawarehouses;
    """

    warehouses = []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql_query)
            warehouses = [row["warehouse"] for row in rows]
            warehouses.sort()
    except Exception as e:
        logger.exception(f"Ошибка получения складов в podsort_view: {e}")

    return warehouses


def get_orders_with_filter(nmid_query_filter: str, warehouse_filter: List[str], period) -> Dict:
    """
    Вызывается только если передан warehouse_filter
    Распределяет все заказы для каждого артикула на склады
    Args:
        nmid_query_filter: переданные артикулы в формате "o.nmid IN (356102564)" либо все артикулы
        warehouse_filter: переданные склады
        period: дата начала заказов в формате 2025-ММ-ДД 23:59:59

    Returns: возвращает словарь в виде {356102564: {'Коледино': 682, 'Казань': 410}}

    """
    conn = connect_to_database()

    sql_query = f"""
                        WITH region_warehouse_min AS (
                            SELECT
                                area,
                                (SELECT key
                                 FROM jsonb_each_text(warehouses)
                                 WHERE key = ANY(%s)
                                 ORDER BY (value)::int
                                 LIMIT 1) AS min_warehouse
                            FROM myapp_areawarehouses
                        ),
                        orders_with_warehouse AS (
                            SELECT
                                o.nmid,
                                rwm.min_warehouse
                            FROM myapp_orders o
                            JOIN region_warehouse_min rwm
                                ON o.regionname = rwm.area
                            WHERE
                                o.date >= %s
                            AND {nmid_query_filter}
                        )
                        SELECT
                            nmid,
                            COALESCE(min_warehouse, 'Неопределено') AS warehouse_with_min_value,
                            COUNT(*) AS order_count
                        FROM orders_with_warehouse
                        GROUP BY nmid, min_warehouse
                        ORDER BY order_count DESC;
                    """

    with conn.cursor() as cursor:
        try:
            cursor.execute(sql_query, (warehouse_filter, period))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            dict_rows = [dict(zip(columns, row)) for row in rows]

            result = defaultdict(dict)
            for entry in dict_rows:
                nmid = entry['nmid']
                warehouse = entry['warehouse_with_min_value']
                count = entry['order_count']
                result[nmid][warehouse] = count

            orders_with_filter = dict(result)
        except Exception as e:
            logger.exception(f"Сбой при выполнении podsort_view при получении склад-область. Error: {e}")
        finally:
            conn.close()

    # logger.info(f"все заказы: {orders_with_filter}")
    return orders_with_filter


async def get_warh_stock(pool, name_column_available, nmid_query):
    sql_query = f"""
            SELECT
                nmid,
                warehousename,
                {name_column_available} AS available,
                SUM(quantity) AS total_quantity
            FROM myapp_stocks
            WHERE {nmid_query}
            GROUP BY
                nmid, warehousename, {name_column_available}
        """

    warh_stock = {}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql_query)

            result = defaultdict(dict)
            for row in rows:
                nmid = row['nmid']
                warehouse = row['warehousename']
                if warehouse == "Тула":
                    warehouse = "Алексин"
                available = row.get('available', 0)
                total_quantity = row.get('total_quantity', 0)
                result[nmid][warehouse] = {
                    'available': available,
                    'total_quantity': total_quantity
                }

            warh_stock = dict(result)

    except Exception as e:
        logger.exception(f"Сбой при выполнении podsort_view при получении остатков. Error: {e}")

    return warh_stock


def _podsort_view(
        orders_with_filter, articles, warh_stock, period_ord, all_orders, warehouses, current_ids, parametrs,
        flag: bool, export_mode
):
    """
    Если flag то функция отрабатывает со складами
    """
    conn = None
    try:

        turnover_periods = [a for a in range(25, 71, 5)] # оборачиваемость для фронта
        order_periods = [3, 7, 14, 30] # заказы для фронта
        page_sizes = [5, 10, 20, 50, 100] # кол-во отоброажаемых строк
        abc_vars = ["Все товары", "A", "B", "C", "Новинки"] # фильтр
        nmid_filter = parametrs["nmid_filter"]

        warehouse_filter = parametrs["warehouse_filter"] if flag else ""

        alltags_filter = parametrs["alltags_filter"]
        per_page = parametrs["per_page"]
        page_number = parametrs["page_number"]

        sort_by = parametrs["sort_by"]
        order = parametrs["order"]
        abc_filter = parametrs["abc_filter"]

        turnover_change = parametrs["turnover_change"]

        conn = connect_to_database()

        all_response = {}

        try:
            for art, index in articles.items():
                if not all_response.get(art):
                    all_response[art] = {}
                if alltags_filter:
                    if not set(index["tag_ids"]) & set(
                            alltags_filter):  # если два массива не имеют хотя бы одну строку общую
                        if art in all_response:
                            all_response.pop(art)
                        continue

                all_response[art]["id"] = index["id"]
                all_response[art]["article"] = art
                all_response[art]["cloth"] = index["cloth"]
                all_response[art]["i_color"] = index["i_color"]
                all_response[art]["vendorcode"] = index["vendorcode"]
                all_response[art]["tags"] = index["tag_ids"]

                low_vendor = index["vendorcode"].lower()
                if "11ww" in low_vendor or "3240" in low_vendor:
                    all_response[art]["i_size"] = "3240"
                elif "22ww" in low_vendor or "3270" in low_vendor:
                    all_response[art]["i_size"] = "3270"
                elif "33ww" in low_vendor or "3250" in low_vendor:
                    all_response[art]["i_size"] = "3250"
                elif "44ww" in low_vendor or "3260" in low_vendor:
                    all_response[art]["i_size"] = "3260"
                elif "55ww" in low_vendor or "4240" in low_vendor:
                    all_response[art]["i_size"] = "4240"
                elif "66ww" in low_vendor or "4250" in low_vendor:
                    all_response[art]["i_size"] = "4250"
                elif "77ww" in low_vendor or "4260" in low_vendor:
                    all_response[art]["i_size"] = "4260"
                elif "88ww" in low_vendor or "4270" in low_vendor:
                    all_response[art]["i_size"] = "4270"
                elif "2240" in low_vendor:
                    all_response[art]["i_size"] = "2240"
                elif "2250" in low_vendor:
                    all_response[art]["i_size"] = "2250"
                elif "2260" in low_vendor:
                    all_response[art]["i_size"] = "2260"
                elif "2270" in low_vendor:
                    all_response[art]["i_size"] = "2270"
                elif "5240" in low_vendor:
                    all_response[art]["i_size"] = "5240"
                elif "5250" in low_vendor:
                    all_response[art]["i_size"] = "5250"
                elif "5260" in low_vendor:
                    all_response[art]["i_size"] = "5260"
                elif "5270" in low_vendor:
                    all_response[art]["i_size"] = "5270"
                elif "6240" in low_vendor:
                    all_response[art]["i_size"] = "6240"
                elif "6250" in low_vendor:
                    all_response[art]["i_size"] = "6250"
                elif "6260" in low_vendor:
                    all_response[art]["i_size"] = "6260"
                elif "6270" in low_vendor:
                    all_response[art]["i_size"] = "6270"

                all_response[art]["ABC"] = "формула"
                all_response[art]["turnover_total"] = 0

                all_response[art]["subitems"] = []
                all_response[art]["orders"] = 0
                all_response[art]["stock"] = 0

                if all_orders.get(art):
                    for i_key, i_val in warh_stock.get(art, {}).items():
                        if i_key not in all_orders[art].keys() and i_val["total_quantity"]:
                            all_orders[art][i_key] = 0
                    for warh, i_order in all_orders[art].items():
                        all_response[art]["orders"] += i_order
                        all_response[art]["stock"] += warh_stock[art][warh].get("total_quantity", 0) or 0 if (
                                    warh_stock.get(art) and warh_stock[art].get(warh)) else 0
                        all_response[art]["subitems"].append(
                            {
                                "warehouse": warh,
                                "order": i_order,
                                "stock": warh_stock[art][warh].get("total_quantity", 0) or 0 if (
                                            warh_stock.get(art) and warh_stock[art].get(warh)) else 0,
                                "time_available": warh_stock[art][warh].get("available") or 0 if (
                                            warh_stock.get(art) and warh_stock[art].get(warh)) else 0,
                                "turnover": 0,
                                "rec_delivery": 0,
                                "order_for_change_war": 0,
                            }
                        )
                        if warehouse_filter and orders_with_filter[art].get(warh):
                            all_response[art]["subitems"][-1]["order_for_change_war"] = orders_with_filter[art][warh]
                    # logger.info(f"all_response в _posdort: {all_response}")
                    if warehouse_filter:
                        warehouses_in_all_orders = set(all_orders[art].keys())  # склады из общих данных
                        warehouses_in_orders_with_filter = set(orders_with_filter[art].keys()) # склады из общих данных
                        missing_warehouses = warehouses_in_orders_with_filter - warehouses_in_all_orders # склады которые должны быть но их нет
                        for mis_war in missing_warehouses:
                            all_response[art]["subitems"].append(
                                {
                                    "warehouse": mis_war,
                                    "order": 0,
                                    "stock": warh_stock[art][mis_war].get("total_quantity", 0) or 0 if (
                                            warh_stock.get(art) and warh_stock[art].get(mis_war)) else 0,
                                    "time_available": warh_stock[art][mis_war].get("available") or 0 if (
                                            warh_stock.get(art) and warh_stock[art].get(mis_war)) else 0,
                                    "turnover": 0,
                                    "rec_delivery": 0,
                                    "order_for_change_war": orders_with_filter[art][mis_war],
                                }
                            )

                    if warehouse_filter and (order_for_change_war := orders_with_filter[art].get("Неопределено")):
                        all_response[art]["subitems"].append(
                            {
                                "warehouse": "Неопределено",
                                "order": order_for_change_war,
                                "stock": 0,
                                "time_available": 0,
                                "turnover": 0,
                                "rec_delivery": 0,
                            }
                        )
        except Exception as e:
            logger.error(f"Ошибка в обработке итоговых данных {e}")

        sql_nmid = """
            SELECT p.nmid as nmid, p.vendorcode as vendorcode 
            FROM myapp_nmids p 
            JOIN myapp_wblk wblk 
            ON p.lk_id = wblk.id 
            WHERE """ + f"p.nmid IN ({', '.join(map(str, current_ids))})"

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_nmid)
                res_nmids = cursor.fetchall()
                columns_nmids = [desc[0] for desc in cursor.description]
        except Exception as e:
            logger.error(f"Ошибка при запросе артикулов и vendorcode {e}")

        nmids = [dict(zip(columns_nmids, row)) for row in res_nmids]
        combined_list = [
            {
                "nmid": item['nmid'],
                "vendorcode": item['vendorcode'],
            }
            for item in nmids
        ]

        filter_response = get_filter_by_articles(current_ids, clothes=True, sizes=True, colors=True)
        filter_options_without_color = filter_response["cloth"]
        filter_options_sizes = filter_response["sizes"]
        filter_options_colors = filter_response["colors"]

        try:
            for key, value in all_response.items():
                try:
                    all_response[key]["turnover_total"] = int(
                        all_response[key]["stock"] / (all_response[key]["orders"] / period_ord)) \
                        if all_response[key]["orders"] else all_response[key]["stock"]
                except Exception as e:
                    logger.error(f"Ошибка {e} в первом блоке {all_response[key]['orders']} {period_ord}")
                    raise Exception(e)
                all_response[key]["color"] = "green"
                if all_response[key]["subitems"]:
                    all_response[key]["subitems"].sort(
                        key=lambda x: x["order"],
                        reverse=True
                    )
                    # logger.info(f"all_response перед определением рек поставки: {all_response}")
                    for index, i in enumerate(all_response[key]["subitems"]):
                        try:
                            # считаем рек поставку для складов
                            if not warehouse_filter:
                                try:
                                    all_response[key]["subitems"][index]["rec_delivery"] = int(
                                        all_response[key]["subitems"][index][
                                            "order"] / period_ord * turnover_change -
                                        all_response[key]["subitems"][index]["stock"]
                                    )
                                    all_response[key]["subitems"][index]["turnover"] = int(
                                        all_response[key]["subitems"][index]["stock"] / (
                                                    all_response[key]["subitems"][index]["order"] / period_ord)) \
                                        if all_response[key]["subitems"][index]["order"] else \
                                    all_response[key]["subitems"][index]["stock"]
                                except Exception as e:
                                    logger.error(f"Ошибка {e} во втором блоке {turnover_change} {period_ord}")
                                    raise Exception(e)
                                if i.get("warehouse") == "Неопределено":
                                    all_response[key]["subitems"][index]["order"] = 0
                            else:
                                if i.get("warehouse") == "Неопределено":
                                    all_response[key]["subitems"][index]["order_for_change_war"] = \
                                    all_response[key]["subitems"][index]["order"]
                                    all_response[key]["subitems"][index]["order"] = 0

                                try:
                                    all_response[key]["subitems"][index]["turnover"] = int(
                                        all_response[key]["subitems"][index]["stock"] / (
                                                    all_response[key]["subitems"][index]["order_for_change_war"] / period_ord)) \
                                        if all_response[key]["subitems"][index]["order_for_change_war"] else \
                                    all_response[key]["subitems"][index]["stock"]
                                except Exception as e:
                                    logger.error(f"Ошибка при расчете оборачиваемости когда выбраны склады {e}")
                                    raise Exception(e)

                                try:
                                    all_response[key]["subitems"][index]["rec_delivery"] = int(
                                        all_response[key]["subitems"][index][
                                            "order_for_change_war"] / period_ord * turnover_change -
                                        all_response[key]["subitems"][index]["stock"]
                                    ) if all_response[key]["subitems"][index]["order_for_change_war"] else 0
                                except Exception as e:
                                    logger.error(f"Ошибка {e} в третьем блоке {turnover_change} {period_ord}")
                                    raise Exception(e)
                            # ниже просто цвета присваиваем без делений
                            if all_response[key]["subitems"][index]["rec_delivery"] <= -100 or \
                                    all_response[key]["subitems"][index]["rec_delivery"] >= 100:
                                all_response[key]["subitems"][index]["color"] = "red"
                                all_response[key]["color"] = "red" if all_response[key]["turnover_total"] < 25 else "white"
                            elif 40 <= all_response[key]["subitems"][index]["rec_delivery"] < 100 or -40 >= \
                                    all_response[key]["subitems"][index]["rec_delivery"] > -100:
                                all_response[key]["subitems"][index]["color"] = "yellow"
                            elif 0 < all_response[key]["subitems"][index]["rec_delivery"] < 40 or -1 >= \
                                    all_response[key]["subitems"][index]["rec_delivery"] > -40:
                                all_response[key]["subitems"][index]["color"] = "green"
                            else:
                                all_response[key]["subitems"][index]["color"] = "white"
                        except Exception as e:
                            raise Exception(f"Ошибка: {e}. Данные: {all_response[key]['subitems']}")
                    # logger.info(f"all_response после определения рек поставки: {all_response}")
            items = abc_classification(all_response)

            if sort_by in ("turnover_total", "ABC", "vendorcode", "orders", "stock", "cloth", "i_size", "i_color"):
                descending = False if order == "asc" else True
                items = sorted_by(items, sort_by, descending)

            if abc_filter and abc_filter != "Все товары":
                items = filter_by(items, abc_filter)

            items = list(items.values())

            # чистим массив у которого пустые вложения по складам
            items = [item for item in items if item["subitems"]]

            try:
                if export_mode:
                    """ Здесь возвращаем данные для формирования Excel """
                    per_page = len(items)
                    paginator = Paginator(items, per_page)
                    page_obj = paginator.get_page(1)
                    return {"items": page_obj}
            except Exception as e:
                logger.error(f"Ошибка возврата данных при экспорте {e}")
                raise Exception(e)

            paginator = Paginator(items, per_page)
            page_obj = paginator.get_page(page_number)
        except Exception as e:
            logger.exception(f"Ошибка при вторичной обработке данных в podsort_view: {e}")
            page_obj = []
            paginator = None

        sql_query = """SELECT DISTINCT tag FROM myapp_tags"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                alltags = [row[0] for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Сбой при получении всех тегов. Error: {e}")

        try:
            all_articles = nmids_db.objects.values('nmid', 'is_active')
            all_articles = [{'nmid': item['nmid'], 'status': item['is_active']} for item in all_articles]
        except Exception as e:
            logger.error(f"Ошибка получения всех артикулов в podsort_view: {e}")

        return {
                "warehouses": warehouses,
                "warehouse_filter": warehouse_filter,
                "alltags_filter": alltags_filter,
                "nmids": combined_list,
                "nmid_filter": nmid_filter,
                "without_color_filter": parametrs['without_color_filter'],
                "sizes_filter": parametrs['sizes_filter'],
                "colors_filter": parametrs['colors_filter'],
                "items": page_obj,
                "paginator": paginator,
                "turnover_periods": turnover_periods,
                "order_periods": order_periods,
                "period_ord": period_ord,
                "turnover_change": turnover_change,
                "page_sizes": page_sizes,
                "per_page": per_page,
                "sort_by": sort_by,
                "abc_filter": abc_filter,
                "abc_vars": abc_vars,
                "order": order,
                "filter_options_without_color": filter_options_without_color,
                "filter_options_sizes": filter_options_sizes,
                "filter_options_colors": filter_options_colors,
                "alltags": alltags,
                "our_g": parametrs['our_g'],
                "category_g": parametrs['category_g'],
                "all_articles": all_articles,
            }
    finally:
        if conn:
            conn.close()


def get_all_filters(
        nmid_filter,
        without_color_filter,
        sizes_filter,
        colors_filter
)->list:
    wc_filter = (
        without_color_filter[0].split(',')
        if without_color_filter and without_color_filter[0].strip() not in ['', '[]']
        else []
    )

    sz_filter = (
        sizes_filter[0].split(',')
        if sizes_filter and sizes_filter[0].strip() not in ['', '[]']
        else []
    )

    cl_filter = (
        colors_filter[0].split(',')
        if colors_filter and colors_filter[0].strip() not in ['', '[]']
        else []
    )
    all_filters = [set(i) for i in [nmid_filter, wc_filter, sz_filter, cl_filter] if i]
    return all_filters