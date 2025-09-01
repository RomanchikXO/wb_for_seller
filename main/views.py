import copy
from typing import List
import multiprocessing as mp
from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponse, HttpResponseRedirect
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font
import json
from io import BytesIO
from parsers.wildberies import get_uuid
from tasks.set_price_on_wb_from_repricer import get_marg, get_price_with_all_disc

import csv
from myapp.models import Price, Stocks, Repricer, WbLk, Tags, nmids as nmids_db, Addindicators
from django.shortcuts import render
from decorators import login_required_cust
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.urls import reverse
from database.DataBase import connect_to_database
from datetime import datetime, timedelta

import re
from collections import defaultdict

import logging
from context_logger import ContextLogger
from myapp.models import CustomUser
import docker

logger = ContextLogger(logging.getLogger("parsers"))


def get_current_nmids()-> List[int]:
    """
    получаем массив артикулов которые нужны селлеру
    Returns:

    """
    active_nmids = nmids_db.objects.filter(is_active=True).values_list('nmid', flat=True)
    active_nmids_list = list(active_nmids)
    return active_nmids_list


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


def filter_by(items: dict, filter_param: str):
    filter_items = dict(
        filter(lambda item: item[1]["ABC"] == filter_param, items.items())
    )
    return filter_items


def sorted_by(items: dict, sort_by: str, descending: bool = False) -> dict:
    """
    Сортирует словарь items по значению ключа sort_by во вложенных словарях.

    :param items: словарь, где ключ — артикул, значение — словарь с ключом sort_by
    :param descending: если True — сортировка по убыванию, иначе — по возрастанию
    :return: новый отсортированный словарь
    """
    sorted_items = dict(
        sorted(
            items.items(),
            key=lambda item: item[1].get(sort_by, 0),
            reverse=descending
        )
    )
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

    return dict(sorted_items)


@login_required_cust
def main_view(request):
    data = []

    client = docker.from_env()
    containers = client.containers.list(all=True)

    for container in containers:
        data.append({
            'name': container.name,
            'id': container.short_id,
            'status': container.status,
        })
    return render(
        request,
        'main.html',
        {'data': data}
    )


@require_POST
@login_required_cust
def restart_container_view(request, container_id):
    if request.method == "POST":
        try:
            client = docker.from_env()
            container = client.containers.get(container_id)
            container.restart()
            logger.info(f"Контейнер {container_id} перезапущен пользователем {request.user}")
        except Exception as e:
            logger.error(f"Ошибка при перезапуске контейнера {container_id}: {str(e)}")
    return HttpResponseRedirect(reverse('main'))


@require_POST
@login_required_cust
def stop_container_view(request, container_id):
    if request.method == "POST":
        try:
            client = docker.from_env()
            container = client.containers.get(container_id)
            container.stop()
            logger.info(f"Контейнер {container_id} остановлен пользователем {request.user}")
        except Exception as e:
            logger.error(f"Ошибка при остановке контейнера {container_id}: {str(e)}")
    return HttpResponseRedirect(reverse('main'))


@login_required_cust
def repricer_view(request):
    page_sizes = [5, 10, 20, 50, 100]
    per_page = int(request.GET.get('per_page', 10))
    page_number = int(request.GET.get('page', 1))
    nmid_filter = request.GET.getlist('nmid')  # фильтр по артикулвм
    sort_by = request.GET.get('sort_by', '')  # значение по умолчанию
    order = request.GET.get('order', 'asc')  # asc / desc

    # Валидные поля сортировки (ключ = название в шаблоне, значение = поле в ORM)
    valid_sort_fields = {
        'redprice': 'redprice',
        'quantity': 'quantity',
        'spp': 'spp',
        'status': 'is_active',
    }

    sort_field = valid_sort_fields.get(sort_by)

    try:
        sql_query = """
            SELECT
                p.lk_id as lk_id,
                p.nmid as nmid,
                p.vendorcode as vendorcode,
                COALESCE(p.redprice, 0) as redprice,
                r.keep_price as keep_price,
                r.price_plan as price_plan,
                r.marg_or_price as marg_or_price,
                p.spp as spp,
                COALESCE(r.is_active, FALSE) AS is_active,
                COALESCE(s.total_quantity, 0) AS quantity
            FROM
                myapp_price p
            LEFT JOIN myapp_repricer r ON p.lk_id = r.lk_id AND p.nmid = r.nmid
            LEFT JOIN (
                SELECT
                    lk_id,
                    nmid,
                    SUM(quantity) AS total_quantity
                FROM
                    myapp_stocks
                GROUP BY
                    lk_id, nmid
            ) s ON p.lk_id = s.lk_id AND p.nmid = s.nmid
        """

        sql_nmid = ("SELECT p.nmid as nmid, p.vendorcode as vendorcode "
                    "FROM myapp_price p "
                    "JOIN myapp_wblk wblk "
                    "ON p.lk_id = wblk.id")
        conn = connect_to_database()
        with conn.cursor() as cursor:
            cursor.execute(sql_nmid, )
            res_nmids = cursor.fetchall()

        columns_nmids = [desc[0] for desc in cursor.description]
        nmids = [dict(zip(columns_nmids, row)) for row in res_nmids]
        combined_list = [
            {
                "nmid": item['nmid'],
                "vendorcode": item['vendorcode'],
            }
            for item in nmids
        ]
        # tail_filter_options = get_group_nmids(combined_list)
        current_ids = get_current_nmids()
        tail_filter_options = get_filter_by_articles(current_ids, size_color=True)["size_color"]

        # Добавляем фильтрацию по nmid, если она задана
        if nmid_filter:
            sql_query += " WHERE p.nmid IN (%s)" % ','.join(['%s'] * len(nmid_filter))

        # Определяем порядок сортировки
        if sort_field:
            if sort_by == 'quantity':
                sql_query += """
                        ORDER BY
                            (CASE WHEN s.total_quantity = 0 THEN 1 ELSE 0 END),
                            %s %s
                    """ % (sort_field, 'ASC' if order == 'asc' else 'DESC')
            elif sort_by == 'redprice':
                sql_query += """
                        ORDER BY
                            (CASE WHEN p.redprice IS NULL THEN 1 ELSE 0 END),
                            %s %s
                    """ % (sort_field, 'ASC' if order == 'asc' else 'DESC')
            elif sort_by == 'is_active':
                sql_query += """
                        ORDER BY
                            r.is_active %s
                    """ % ('ASC' if order == 'asc' else 'DESC')
            elif sort_by == 'spp':
                sql_query += """
                        ORDER BY
                            (CASE WHEN p.spp IS NULL THEN 1 ELSE 0 END),
                            %s %s
                    """ % (sort_field, 'ASC' if order == 'asc' else 'DESC')

        # Выполнение SQL запроса и получение данных
        conn = connect_to_database()
        with conn.cursor() as cursor:
            cursor.execute(sql_query, nmid_filter)
            rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        dict_rows = [dict(zip(columns, row)) for row in rows]

        paginator = Paginator(dict_rows, per_page)
        page_obj = paginator.get_page(page_number)


    except Exception as e:
        logger.error(f"Error in repricer_view: {e}")
        page_obj = []
        nmids = []
        paginator = None

    status_rep = Price.objects.order_by('id').values_list('main_status', flat=True).first()

    return render(request, 'repricer.html', {
        "page_obj": page_obj,
        "per_page": per_page,
        "paginator": paginator,
        "page_sizes": page_sizes,
        "nmids": combined_list,
        "nmid_filter": nmid_filter,
        "sort_by": sort_by,
        "order": order,
        "tail_filter_options": tail_filter_options,
        "status_rep": status_rep,
    })


@require_POST
@login_required_cust
def set_status_rep(request):
    payload = json.loads(request.body)

    try:
        Price.objects.all().update(main_status=payload['status'])
    except Exception as e:
        logger.error(f"Ошибка при переключении включении/отключении репрайсера в set_status_rep: {e}")
        return JsonResponse({'status': 'error'})
    return JsonResponse({'status': 'ok'})


@require_POST
@login_required_cust
def repricer_save(request):
    payload = json.loads(request.body)
    item = payload.get('item', [])

    try:
        if not item["keep_price"].isdigit(): item["keep_price"] = 0
        if not item["price_plan"].isdigit(): item["price_plan"] = 0
        lk_instance = WbLk.objects.get(id=item['lk_id'])
        Repricer.objects.update_or_create(
            lk=lk_instance,
            nmid=item['nmid'],
            defaults={
                'keep_price': item['keep_price'],
                'price_plan': item['price_plan'],
                'marg_or_price': item['marg_or_price'],
                'is_active': item['is_active']
            }
        )
    except Exception as e:
        logger.error(f"Error in repricer_save: {e}")

    return JsonResponse({'status': 'ok', 'received': len(item)})


@require_POST
@login_required_cust
def get_marg_api(request):
    payload = json.loads(request.body)
    price = payload.get('price')
    nmid = payload.get('nmid')

    sql_query = f"""
        SELECT * FROM myapp_price WHERE nmid = {int(nmid)}
    """
    conn = connect_to_database()
    with conn.cursor() as cursor:
        cursor.execute(sql_query, )
        res_nmids = cursor.fetchall()

    columns_nmids = [desc[0] for desc in cursor.description]
    nmids = [dict(zip(columns_nmids, row)) for row in res_nmids]
    nmids = nmids[0]

    try:
        price_with_disc, black_price = get_price_with_all_disc(price, nmids["spp"], nmids["discount"],
                                                               nmids["wallet_discount"])
        response = get_marg(price_with_disc, nmids["discount"], nmids["cost_price"], nmids["reject"],
                            nmids["commission"], nmids["acquiring"], nmids["nds"], nmids["usn"], nmids["drr"])
    except Exception as e:
        logger.error(f"Error in get_marg_api: {e}")
        return JsonResponse({'status': 'error', 'message': 'Ошибка в данных'})

    return JsonResponse({'status': 'ok', 'received': response})


def _podsort_view(parametrs, flag: bool):
    """
    Если flag то функция отрабатывает со складами
    """
    current_ids = get_current_nmids()

    now_msk = datetime.now() + timedelta(hours=3)
    yesterday_end = now_msk.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    tree_days_ago = yesterday_end - timedelta(days=3)
    seven_days_ago = yesterday_end - timedelta(days=7)
    two_weeks_ago = yesterday_end - timedelta(weeks=2)
    thirty_days_ago = yesterday_end - timedelta(days=30)

    turnover_periods = [a for a in range(25, 71, 5)]
    order_periods = [3, 7, 14, 30]

    value = parametrs["value"]

    page_sizes = [5, 10, 20, 50, 100]
    abc_vars = ["Все товары", "A", "B", "C", "Новинки"]
    nmid_filter = parametrs["nmid_filter"]

    without_color_filter = parametrs["without_color_filter"]
    wc_filter = (
        without_color_filter[0].split(',')
        if without_color_filter and without_color_filter[0].strip() not in ['', '[]']
        else []
    )

    sizes_filter = parametrs["sizes_filter"]
    sz_filter = (
        sizes_filter[0].split(',')
        if sizes_filter and sizes_filter[0].strip() not in ['', '[]']
        else []
    )
    colors_filter = parametrs["colors_filter"]
    cl_filter = (
        colors_filter[0].split(',')
        if colors_filter and colors_filter[0].strip() not in ['', '[]']
        else []
    )

    warehouse_filter = parametrs["warehouse_filter"] if flag else ""

    alltags_filter = parametrs["alltags_filter"]
    per_page = parametrs["per_page"]
    page_number = parametrs["page_number"]

    sort_by = parametrs["sort_by"]
    order = parametrs["order"]
    abc_filter = parametrs["abc_filter"]

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
    params = [period]

    turnover_change = parametrs["turnover_change"]

    # получаем все склады
    sql_query = """
            SELECT DISTINCT jsonb_object_keys(warehouses) AS warehouse
            FROM myapp_areawarehouses;
        """

    conn = connect_to_database()
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql_query, )
            rows = cursor.fetchall()
        except Exception as e:
            logger.exception(f"Ошибка получаения складов в podsort_view: {e}")
        warehouses = [row[0] for row in rows]

    all_filters = [set(i) for i in [nmid_filter, wc_filter, sz_filter, cl_filter] if i]

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
                    o.date >= '{period}'
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
    conn = connect_to_database()
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql_query, )
            rows = cursor.fetchall()
        except Exception as e:
            logger.exception(f"Сбой при выполнении podsort_view для заказов. Error: {e}")
        columns = [desc[0] for desc in cursor.description]
        dict_rows = [dict(zip(columns, row)) for row in rows]

        result = defaultdict(dict)
        for entry in dict_rows:
            nmid = entry['nmid']
            warehouse = entry['warehouse_with_min_value']
            count = entry['order_count']
            result[nmid][warehouse] = count

        all_orders = dict(result)

    if warehouse_filter:
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
        conn = connect_to_database()
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql_query, (warehouse_filter, period))
                rows = cursor.fetchall()
            except Exception as e:
                logger.exception(f"Сбой при выполнении podsort_view при получении склад-область. Error: {e}")
            columns = [desc[0] for desc in cursor.description]
            dict_rows = [dict(zip(columns, row)) for row in rows]

            result = defaultdict(dict)
            for entry in dict_rows:
                nmid = entry['nmid']
                warehouse = entry['warehouse_with_min_value']
                count = entry['order_count']
                result[nmid][warehouse] = count

            orders_with_filter = dict(result)

    # остатки и кол-во дней в наличии
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
    conn = connect_to_database()
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql_query, params)
            rows = cursor.fetchall()
        except Exception as e:
            logger.exception(f"Сбой при выполнении podsort_view при получении остатков. Error: {e}")
        columns = [desc[0] for desc in cursor.description]
        dict_rows = [dict(zip(columns, row)) for row in rows]

        result = defaultdict(dict)
        for entry in dict_rows:
            nmid = entry['nmid']
            warehouse = entry['warehousename'] if entry['warehousename'] != "Тула" else "Алексин"
            available = entry.get('available', 0)
            total_quantity = entry.get('total_quantity', 0)
            result[nmid][warehouse] = {'available': available, 'total_quantity': total_quantity}

        warh_stock = dict(result)

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
    conn = connect_to_database()
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql_query, params)
            rows = cursor.fetchall()
        except Exception as e:
            logger.exception(f"Сбой при выполнении podsort_view при получении артикул, id, ткань и цвет. Error: {e}")
        articles = {row[0]: {"id": row[1], "cloth": row[2], "i_color": row[3], "vendorcode": row[4], "tag_ids": row[5]}
                    for row in rows}

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

    sql_nmid = ("SELECT p.nmid as nmid, p.vendorcode as vendorcode "
                "FROM myapp_price p "
                "JOIN myapp_wblk wblk "
                "ON p.lk_id = wblk.id "
                f"WHERE p.nmid IN ({', '.join(map(str, current_ids))})")
    conn = connect_to_database()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_nmid, )
            res_nmids = cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при запросе артикулов и vendorcode {e}")

    columns_nmids = [desc[0] for desc in cursor.description]
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
            all_response[key]["turnover_total"] = int(
                all_response[key]["stock"] / (all_response[key]["orders"] / period_ord)) \
                if all_response[key]["orders"] else all_response[key]["stock"]
            all_response[key]["color"] = "green"
            if all_response[key]["subitems"]:
                all_response[key]["subitems"].sort(
                    key=lambda x: x["order"],
                    reverse=True
                )

                for index, i in enumerate(all_response[key]["subitems"]):
                    # по просьбе заказчика от 18.08.25 учитывать нераспределенные заказы
                    # if i.get("warehouse") == "Неопределено": continue
                    try:
                        # считаем рек поставку для складов
                        if not warehouse_filter:
                            all_response[key]["subitems"][index]["rec_delivery"] = int(
                                all_response[key]["subitems"][index][
                                    "order"] / period_ord * turnover_change -
                                all_response[key]["subitems"][index]["stock"]
                            )
                            if i.get("warehouse") == "Неопределено":
                                all_response[key]["subitems"][index]["order"] = 0
                        else:
                            if i.get("warehouse") == "Неопределено":
                                all_response[key]["subitems"][index]["order_for_change_war"] = \
                                all_response[key]["subitems"][index]["order"]
                                all_response[key]["subitems"][index]["order"] = 0

                            all_response[key]["subitems"][index]["rec_delivery"] = int(
                                all_response[key]["subitems"][index][
                                    "order_for_change_war"] / period_ord * turnover_change -
                                all_response[key]["subitems"][index]["stock"]
                            ) if all_response[key]["subitems"][index]["order_for_change_war"] else 0

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
                    except Exception:
                        raise Exception(all_response[key]["subitems"])
        items = abc_classification(all_response)

        if sort_by in ("turnover_total", "ABC", "vendorcode", "orders", "stock", "cloth", "i_size", "i_color"):
            descending = False if order == "asc" else True
            items = sorted_by(items, sort_by, descending)

        if abc_filter and abc_filter != "Все товары":
            items = filter_by(items, abc_filter)

        items = list(items.values())

        # чистим массив у которого пустые вложения по складам
        items = [item for item in items if item["subitems"]]

        paginator = Paginator(items, per_page)
        page_obj = paginator.get_page(page_number)
    except Exception as e:
        logger.error(f"Ошибка при вторичной обработке данных в podsort_view: {e}")
        page_obj = []
        paginator = None

    sql_query = """SELECT DISTINCT tag FROM myapp_tags"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query, params)
            rows = cursor.fetchall()
            alltags = [row[0] for row in rows] if rows else []
    except Exception as e:
        logger.error(f"Сбой при получении всех тегов. Error: {e}")

    try:
        all_articles = nmids_db.objects.values('nmid', 'is_active')
        all_articles = [{'nmid': item['nmid'], 'status': item['is_active']} for item in all_articles]
    except Exception as e:
        logger.error(f"Ошибка получения всех артикулов в podsort_view: {e}")

    try:
        our_g, category_g = Addindicators.objects.values_list('our_g', 'category_g').get(id=1)
    except Addindicators.DoesNotExist:
        our_g, category_g = 0, 0

    return {
            "warehouses": warehouses,
            "warehouse_filter": warehouse_filter,
            "alltags_filter": alltags_filter,
            "nmids": combined_list,
            "nmid_filter": nmid_filter,
            "without_color_filter": without_color_filter,
            "sizes_filter": sizes_filter,
            "colors_filter": colors_filter,
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
            "our_g": our_g,
            "category_g": category_g,
            "all_articles": all_articles,
        }


@login_required_cust
def podsort_view(request):
    try:
        session_keys = ['per_page', 'period_ord', 'turnover_change', 'nmid', 'warehouse', 'alltagstb', 'sort_by', 'order',
                        'page', 'abc_filter']
        for key in session_keys:
            value = request.GET.getlist(key) if key in ['nmid', 'warehouse', 'alltagstb'] else request.GET.get(key)
            if value:
                request.session[key] = value
        nmid_filter = request.GET.getlist('nmid', [])
        without_color_filter = request.GET.getlist('wc_filter', "")
        sizes_filter = request.GET.getlist('sz_filter', [])
        colors_filter = request.GET.getlist('cl_filter', [])
        warehouse_filter = request.GET.getlist('warehouse', "")
        alltags_filter = request.GET.getlist('alltagstb', "")
        per_page = int(request.session.get('per_page', int(request.GET.get('per_page', 10))))
        page_number = int(request.session.get('page', int(request.GET.get('page', 1))))
        sort_by = request.session.get('sort_by', request.GET.get("sort_by", ""))  # значение по умолчанию
        order = request.session.get('order', request.GET.get("order", ""))  # asc / desc
        abc_filter = request.session.get('abc_filter', request.GET.get("abc_filter", ""))
        period_ord = int(request.session.get('period_ord', int(request.GET.get('period_ord', 14))))
        turnover_change = int(request.session.get('turnover_change', int(request.GET.get('turnover_change', 40))))

        params = {
            "value": value,
            "nmid_filter": nmid_filter,
            "without_color_filter": without_color_filter,
            "sizes_filter": sizes_filter,
            "colors_filter": colors_filter,
            "warehouse_filter": warehouse_filter,
            "alltags_filter": alltags_filter,
            "per_page": per_page,
            "page_number": page_number,
            "sort_by": sort_by,
            "order": order,
            "abc_filter": abc_filter,
            "period_ord": period_ord,
            "turnover_change": turnover_change,
        }
    except Exception as e:
        logger.error(f"Ошибка приготовления параметров {e}")
        raise

    # Если складов не было возвращаем сразу результат
    try:
        if not warehouse_filter:
            response = _podsort_view(params, False)
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
        with mp.Pool(processes=2) as pool:
            results = pool.starmap(
                _podsort_view,
                [(params, True), (params, False)]
            )

        full_data = results[0]
        short_data = list(results[1]["items"].object_list)

        total_short_rec_del = {} # тут будем хранить общую рек поставку  артикул - сумма
        try:
            for i in short_data:
                if subitems:= i.get("subitems"):
                    for i_sub in subitems:
                        if total_short_rec_del.get(i["article"]):
                            total_short_rec_del[i["article"]] += i_sub["rec_delivery"]
                        else:
                            total_short_rec_del[i["article"]] = i_sub["rec_delivery"]
        except Exception as e:
            raise Exception(f"Ошибка при подсчете total_short_rec_del {e}")

        copy_data = copy.deepcopy(full_data["items"].object_list)
        for index, i in enumerate(list(full_data["items"].object_list)):
            if subitems := i.get("subitems"):
                sum_rec_warh = 0                                                    #сумма поставок когда есть фильтры
                sum_rec_all = sum(list(map(lambda x: x["rec_delivery"], subitems))) #сумма поставок с фильтрами
                coef = total_short_rec_del[i["article"]] / sum_rec_all
                last_index = 0

                try:
                    for _index, art in enumerate(copy_data[index]["subitems"]):
                        art["rec_delivery"] = round(art["rec_delivery"] * coef)
                        if art["rec_delivery"] != 0: last_index = _index
                        sum_rec_warh += art["rec_delivery"]
                except Exception as e:
                    raise Exception(f"Ошибка при формировании sum_rec_warh. Ошибка: {e}")

                try:
                    if sum_rec_warh > total_short_rec_del[i["article"]]:
                        copy_data[index]["subitems"][last_index] -= sum_rec_warh - total_short_rec_del[i["article"]]
                    elif sum_rec_warh < total_short_rec_del[i["article"]]:
                        copy_data[index]["subitems"][last_index] += total_short_rec_del[i["article"]] - sum_rec_warh
                except Exception as e:
                    raise Exception(
                        f"Ошибка в блоке сравнения. Ошибка: {e}. "
                        f"Данные:{sum_rec_warh} {total_short_rec_del[i['article']]}"
                        f"{copy_data[index]['subitems']}... {last_index}"
                    )

        full_data["items"] = copy_data
        return render(
            request,
            "podsort.html",
            full_data
        )
    except Exception as e:
        logger.error(f"Какая то ошибка {e}")


@require_POST
@login_required_cust
def set_stat_nmid(request):
    try:
        data = json.loads(request.body)

        article = data.get("article")
        status = data.get("status")

        if not article or status is None:
            return JsonResponse({"error": "Не переданы обязательные параметры"}, status=400)

        updated = nmids_db.objects.filter(nmid=article).update(is_active=status)
        if updated == 0:
            return JsonResponse({"error": "Артикул не найден"}, status=404)

        return JsonResponse({'status': 'ok', 'message': "Успешно"})

    except json.JSONDecodeError:
        return JsonResponse({"error": "Некорректный JSON"}, status=400)
    except Exception as e:
        logger.error(f"Ошибка обновления статуса артикула {data if 'data' in locals() else ''}: {e}")
        return JsonResponse({"error": str(e)}, status=500)


@require_POST
@login_required_cust
def our_growth(request):
    try:
        data = json.loads(request.body)
        Addindicators.objects.update_or_create(
            id=1,
            defaults={'our_g': data['value']}
        )
        return JsonResponse({'status': 'ok', 'value': data.get('value', 0)})
    except Exception as e:
        logger.error(f"Ошибка обновления нашего роста: {e}")
        return JsonResponse({"error": str(e)}, status=400)


@require_POST
@login_required_cust
def category_growth(request):
    try:
        data = json.loads(request.body)
        Addindicators.objects.update_or_create(
            id=1,
            defaults={'category_g': data['value']}
        )
        return JsonResponse({'status': 'ok', 'value': data.get('value', 0)})
    except Exception as e:
        logger.error(f"Ошибка обновления роста категории: {e}")
        return JsonResponse({"error": str(e)}, status=400)


@require_POST
@login_required_cust
def set_tags(request):
    """
        обновляем теги у товара
    """
    try:
        data = json.loads(request.body)
        for nmid, tags in data.items():
            if tags:
                tag_ids = list(Tags.objects.filter(tag__in=tags).values_list('id', flat=True))
            else:
                tag_ids = []
            nmids_db.objects.filter(nmid=nmid).update(tag_ids=tag_ids)
    except Exception as e:
        logger.error(f"Ошибка добавления тегов к артикулу: {e}")
        return JsonResponse({"error": str(e)})
    return JsonResponse({'status': 'ok'})


@require_POST
@login_required_cust
def add_tag(request):
    try:
        data = json.loads(request.body)
        """
        тут должны добавляться новые теги в базу с тегами 
        """
        Tags.objects.create(tag=data)
    except Exception as e:
        logger.error(f"Ошибка добавления тега в БД с тегами: {e}")
        return JsonResponse({"error": str(e)})
    return JsonResponse({'status': 'ok'})


@require_POST
@login_required_cust
def export_excel(request):
    current_headers = {
        "nmid": "Артикул",
        "vendorcode": "Артикул продавца",
        "redprice": "Красная цена",
        "quantity": "Остаток",
        "spp": "spp",
        "keep_price": "Поддерживать маржу",
        "price_plan": "Поддерживать цену",
        "marg_or_price": "Маржа или Цена",
        "is_active": "Статус",

    }
    if request.method == 'POST':
        data = json.loads(request.body)
        items = data.get("items", [])

        wb = Workbook()
        ws = wb.active
        ws.title = "Repricer"

        # Заголовки (ключи словаря)
        if items:
            headers = list(items[0].keys())[1:]
            current_headers = [current_headers[i] for i in headers]
            ws.append(current_headers)

            for item in items:
                row = [item.get(col, "") if col != "marg_or_price" else ("Маржа" if item.get(col, "") else "Цена") for col in headers]
                ws.append(row)

        # Сохраняем файл в память
        stream = BytesIO()
        wb.save(stream)
        stream.seek(0)

        response = HttpResponse(
            stream,
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=repricer_export.xlsx'
        return response


@require_POST
@login_required_cust
def upload_excel(request):
    file = request.FILES.get('file')
    if not file:
        return JsonResponse({'error': 'Файл не получен'}, status=400)
    if not file.name.endswith('.xlsx'):
        return JsonResponse({'error': 'Допустимы только файлы .xlsx'}, status=400)

    # обработка файла: можно через openpyxl или др.
    wb = load_workbook(filename=file)
    sheet = wb.active

    try:
        data_map = {int(row[0]): int(row[1]) for row in sheet.iter_rows(values_only=True) if row[1]}
        repricer_items = Repricer.objects.filter(nmid__in=data_map.keys())

        for item in repricer_items:
            item.keep_price = data_map[item.nmid]

        Repricer.objects.bulk_update(repricer_items, ['keep_price'])

    except Exception as e:
        return JsonResponse({'error': f'Ошибка при сохранении: {e}'}, status=400)

    return JsonResponse({'status': 'ok'})


@require_POST
@login_required_cust
def export_excel_podsort(request):
    # Создаём книгу и активный лист
    wb = Workbook()
    ws = wb.active
    ws.title = "Подсортировка"

    # Заголовки родительской таблицы
    headers = [
        "Артикул", "Артикул поставщика", "Ткань", "Цвет", "Размер", "Заказы", "Остатки", "АВС по размерам", "Теги", "Оборачиваемость общая"
    ]
    subheaders = ["Склад", "Заказы", "Рек. поставка", "Остатки", "Дней в наличии"]

    row_num = 1
    header_font = Font(bold=True)

    # Пишем заголовки
    for col_num, column_title in enumerate(headers, 1):
        cell = ws.cell(row=row_num, column=col_num)
        cell.value = column_title
        cell.font = header_font

    # Загружаем данные
    data = json.loads(request.body)
    items = data.get("items", [])

    for item in items:
        row_num += 1

        ws.cell(row=row_num, column=1, value=item["article"])
        ws.cell(row=row_num, column=2, value=item["vendorcode"])
        ws.cell(row=row_num, column=3, value=item["cloth"])
        ws.cell(row=row_num, column=4, value=item["i_color"])
        ws.cell(row=row_num, column=5, value=item["i_size"])
        ws.cell(row=row_num, column=6, value=item["orders"])
        ws.cell(row=row_num, column=7, value=item["stock"])
        ws.cell(row=row_num, column=8, value=item["ABC"])
        ws.cell(row=row_num, column=9, value=item["tags"])
        ws.cell(row=row_num, column=10, value=item["turnover_total"])

        # Вложенные subitems
        if item.get("subitems"):
            row_num += 1
            # Подзаголовки
            for col_num, column_title in enumerate(subheaders, 2):  # начинаем с колонки 2
                cell = ws.cell(row=row_num, column=col_num)
                cell.value = column_title
                cell.font = Font(italic=True, bold=True)

            for subitem in item["subitems"]:
                row_num += 1
                ws.cell(row=row_num, column=2, value=subitem["warehouse"])
                ws.cell(row=row_num, column=3, value=subitem["order"])
                ws.cell(row=row_num, column=4, value=subitem["rec_delivery"])
                ws.cell(row=row_num, column=5, value=subitem["stock"])
                ws.cell(row=row_num, column=6, value=subitem["time_available"])

    # Автоширина колонок
    for col in ws.columns:
        max_length = 0
        column = col[0].column
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        adjusted_width = max_length + 2
        ws.column_dimensions[get_column_letter(column)].width = adjusted_width

    stream = BytesIO()
    wb.save(stream)
    stream.seek(0)

    response = HttpResponse(
        stream,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=podsort_export.xlsx'
    return response


@login_required_cust
def margin_view(request):
    return render(
        request,
        'margin.html',
    )

@require_POST
@login_required_cust
def get_margin_data(request):
    now_msk = datetime.now() + timedelta(hours=3)
    first_day = now_msk.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


    sql_query = """
            WITH base AS (
              SELECT
                m.vendorcode,
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
              WHERE рисунок.value IS NOT NULL AND цвет.value IS NOT NULL
            )
            SELECT jsonb_object_agg(main_group, colors) AS result
            FROM (
              SELECT
                main_group,
                jsonb_object_agg(color, vendorcodes) AS colors
              FROM (
                SELECT
                  main_group,
                  color,
                  jsonb_agg(vendorcode) AS vendorcodes
                FROM base
                GROUP BY main_group, color
              ) AS grouped
              GROUP BY main_group
            ) AS final;
        """

    # Выполнение SQL запроса и получение данных
    conn = connect_to_database()
    with conn.cursor() as cursor:
        cursor.execute(sql_query, )
        rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    dict_rows = [dict(zip(columns, row)) for row in rows]

    items = []
    for key_1, value_1 in dict_rows[0]["result"].items():  # например key_1 "МРАМОР" value_1 "белый"
        items.append(
            {
                "id": get_uuid(),
                'name': key_1,
                'revenue_plan': '500',
                'revenue_fact': '400',
                'revenue_trend': '56',
                'margin_fact': '100',
                'drr': '50',
                'how_much_before_plan': '50',
                'revenue_plan_day': '10',
                "children": []
            }
        )
        for key_2, value_2 in value_1.items():  # например key_1 "белый" value_1 [массив артикулов]
            items[-1]["children"].append(
                {
                    "id": get_uuid(),
                    'name': key_2,
                    'revenue_plan': '500',
                    'revenue_fact': '400',
                    'revenue_trend': '56',
                    'margin_fact': '100',
                    'drr': '50',
                    'how_much_before_plan': '50',
                    'revenue_plan_day': '10',
                    "children": []
                }
            )
            for val_3 in value_2:
                items[-1]["children"][-1]["children"].append(
                    {
                        'id': get_uuid(),
                        'name': val_3,
                        'revenue_plan': '500',
                        'revenue_fact': '400',
                        'revenue_trend': '56',
                        'margin_fact': '100',
                        'drr': '50',
                        'how_much_before_plan': '50',
                        'revenue_plan_day': '10',
                    }
                )

    total = {
        'revenue_plan': '500',
        'revenue_fact': '400',
        'revenue_trend': '56',
        'margin_fact': '100',
        'drr': '50',
        'how_much_before_plan': '50',
        'revenue_plan_day': '10',
    }

    dates = [(first_day + timedelta(days=i)).strftime('%d.%m.%Y')
             for i in range((now_msk.date() - first_day.date()).days + 1)]

    return JsonResponse({
        'total': total,
        'items': items,
        'dates': dates,
    })


@login_required_cust
def shipment_view(request):
    page_sizes = [5, 10, 20, 50, 100]

    try:
        sql_query = """
            SELECT * 
            FROM myapp_shipments AS sh
            JOIN myapp_wblk AS wblk
            ON sh.lk_id = wblk.id
        """
        conn = connect_to_database()
        with conn.cursor() as cursor:
            cursor.execute(sql_query, )
            res_nmids = cursor.fetchall()

        columns_nmids = [desc[0] for desc in cursor.description]
        shipments = [dict(zip(columns_nmids, row)) for row in res_nmids]
    except Exception as e:
        logger.error(f"Ошибка получения поставок из БД в shipment_view. Ошибка: {e}")

    return render(
        request,
        'shipment.html',
        {
            "page_sizes": page_sizes,
        }
    )

@login_required_cust
def warehousewb_view(request):
    return render(
        request,
        'warehousewb.html',
    )


@require_POST
@login_required_cust
def get_warehousewb_data(request):
    try:
        sql_query = """
            SELECT 
                bw_agg.incomeid,
                bw_agg.warehousename,
                bw_agg.on_the_way,
                sp_agg.accepted,
                bw_agg.lk_name
            FROM (
                SELECT 
                    bw.incomeid,
                    bw.warehousename,
                    SUM(bw.quantity) AS on_the_way,
                    lk.name AS lk_name
                FROM myapp_betweenwarhouses AS bw
                JOIN myapp_wblk AS lk
                    ON bw.lk_id = lk.id
                GROUP BY bw.incomeid, bw.warehousename, lk.name
            ) AS bw_agg
            LEFT JOIN (
                SELECT 
                    sp."incomeId" AS incomeid,
                    SUM(sp.quantity) AS accepted
                FROM myapp_supplies AS sp
                GROUP BY sp."incomeId"
            ) AS sp_agg
            ON bw_agg.incomeid = sp_agg.incomeid
        """

        conn = connect_to_database()
        with conn.cursor() as cursor:
            cursor.execute(sql_query, )
            res_nmids = cursor.fetchall()

        columns_nmids = [desc[0] for desc in cursor.description]
        shipments = [dict(zip(columns_nmids, row)) for row in res_nmids]
    except Exception as e:
        logger.error(f"Ошибка получения поставок из БД в warehousewb_view. Ошибка: {e}")

    return JsonResponse({
        'page_obj': shipments,
    })


@require_POST
@login_required_cust
def get_warehousewb_add_data(request):
    try:
        try:
            sql_query = """
                SELECT DISTINCT name FROM myapp_wblk
            """
            conn = connect_to_database()
            with conn.cursor() as cursor:
                cursor.execute(sql_query, )
                res_nmids = cursor.fetchall()
            lks_names = [row[0] for row in res_nmids]
        except Exception as e:
            raise Exception(f"Ошибка получения ИП из БД в get_warehousewb_add_data: {e}")

        try:
            sql_query = """
                SELECT DISTINCT "warehouseName" FROM myapp_supplies
            """
            conn = connect_to_database()
            with conn.cursor() as cursor:
                cursor.execute(sql_query, )
                warehouses_data = cursor.fetchall()
            warehouses = [row[0] for row in warehouses_data]
        except Exception as e:
            raise Exception(f"Ошибка получения складов из БД в get_warehousewb_add_data: {e}")
    except Exception as e:
        logger.error(e)

    return JsonResponse({
        'lks_names': lks_names,
        'warehouses': warehouses
    })

@require_POST
@login_required_cust
def warehousewb_submit_supply(request):
    incomeid = request.POST.get('incomeid')
    warehousename = request.POST.get('warehousename')
    lk_name = request.POST.get('lk_name')

    logger.info(f"Получены данные поставки: incomeid={incomeid}, warehousename={warehousename}, lk_name={lk_name}")

    csv_file = request.FILES.get('csv_file')
    try:
        # Читаем CSV как текст (предполагаем UTF-8, можно добавить обработку кодировок)
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.reader(decoded_file)
        rows = list(reader)

        for i, row in enumerate(rows[:10]):  # Логируем первые 10 строк, чтобы не перегружать логи
            logger.info(f"CSV строка {i + 1}: {row}")

    except Exception as e:
        logger.error(f"Ошибка чтения CSV файла: {e}")
        return JsonResponse({'error': 'Ошибка при обработке CSV файла'}, status=400)

    return JsonResponse({'status': 200,})


@csrf_exempt
def google_webhook_view(request):
    try:
        data = json.loads(request.body)
    except Exception as e:
        logger.error(f"Ошибка {e}")


# @require_POST
# @login_required_cust
# def get_dynamic_filters(request):
#     try:
#         data = json.loads(request.body)
#
#         wc_filter = set(data.get("wc_filter", "").split(",")) if data.get("wc_filter") else set()
#         if wc_filter: wc_filter = {int(i) for i in wc_filter}
#         cl_filter = set(data.get("cl_filter", "").split(",")) if data.get("cl_filter") else set()
#         if cl_filter: cl_filter = {int(i) for i in cl_filter}
#         sz_filter = set(data.get("sz_filter", "").split(",")) if data.get("sz_filter") else set()
#         if sz_filter: sz_filter = {int(i) for i in sz_filter}
#
#         filter_options_without_color = data.get("filter_options_without_color", [])
#         filter_options_colors = data.get("filter_options_colors", [])
#         filter_options_sizes = data.get("filter_options_sizes", [])
#
#         # Исходные множества
#         filtered_wc = filter_options_without_color
#         filtered_cl = filter_options_colors
#         filtered_sz = filter_options_sizes
#
#         # Если выбрано что-то, чистим остальные списки по пересечениям
#         if wc_filter: # ткань
#             nmids_allowed = wc_filter
#             if cl_filter:
#                 nmids_allowed &= cl_filter
#             if sz_filter:
#                 nmids_allowed &= sz_filter
#
#             filtered_cl = [
#                 c for c in filter_options_colors
#                 if nmids_allowed & set(c["nmids"])
#             ]
#             filtered_sz = [
#                 s for s in filter_options_sizes
#                 if nmids_allowed & set(s["nmids"])
#             ]
#
#         if cl_filter: # цвет
#             nmids_allowed = cl_filter
#             if wc_filter:
#                 nmids_allowed &= wc_filter
#             if sz_filter:
#                 nmids_allowed &= sz_filter
#
#             filtered_wc = [
#                 w for w in filter_options_without_color
#                 if nmids_allowed & set(w["nmids"])
#             ]
#             filtered_sz = [
#                 s for s in filter_options_sizes
#                 if nmids_allowed & set(s["nmids"])
#             ]
#
#         if sz_filter: # размер
#             nmids_allowed = sz_filter
#
#             if wc_filter:
#                 nmids_allowed &= wc_filter
#             if cl_filter:
#                 nmids_allowed &= cl_filter
#
#             filtered_wc = [
#                 w for w in filter_options_without_color
#                 if nmids_allowed & set(w["nmids"])
#             ]
#             filtered_cl = [
#                 c for c in filter_options_colors
#                 if nmids_allowed & set(c["nmids"])
#             ]
#     except Exception as e:
#         logger.error(f"Ошибка в get_dynamic_filters: {e}")
#         return JsonResponse({'error': e}, status=400)
#
#     return JsonResponse({
#         "filter_options_without_color": filtered_wc,
#         "filter_options_colors": filtered_cl,
#         "filter_options_sizes": filtered_sz
#     })
