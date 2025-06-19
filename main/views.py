from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponse
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font
import json
from io import BytesIO
from parsers.wildberies import get_uuid

import csv
from myapp.models import Price, Stocks, Repricer, WbLk
from django.shortcuts import render
from decorators import login_required_cust
from django.views.decorators.http import require_POST
from database.DataBase import connect_to_database
from datetime import datetime, timedelta

import re
from collections import defaultdict

import logging
from context_logger import ContextLogger
from myapp.models import CustomUser

logger = ContextLogger(logging.getLogger("parsers"))

current_ids = [62999164, 90443540, 90439842, 70497720, 70498242, 90443538, 90439841, 70497721,
               62999167, 90443539, 90439861, 70497722, 207602382, 207603641, 207604857, 207607422,
               62999160, 90443522, 90439860, 70497717, 62999162, 90486202, 90440346, 70497718,
               79716931, 90486206, 90440345, 90489493, 207602381, 207603640, 207604856, 207607421,
               62999159, 90486211, 90440350, 70497716, 62999161, 90486205, 90440347, 90489483,
               188993754, 188994064, 188995051, 296739845, 62999166, 90443534, 90439862, 74512723,
               62999168, 90443541, 90439858, 74512724, 90438134, 90435998, 90343924, 90434376,
               90438131, 90436159, 90344079, 90434843, 90438132, 90436160, 90344704, 90434771,
               207608592, 207609386, 207610332, 207611693, 90438126, 90298281, 90298367, 90298454,
               90438561, 90437129, 90433765, 90435660, 90438564, 90437121, 90422544, 90435376,
               207608591, 207609385, 207610331, 207611692, 90438558, 90437126, 90433760, 90435657,
               90438563, 90437130, 90422563, 90435374, 188995596, 188995742, 188998536, 188998696,
               90438110, 90435997, 90329768, 90343739, 90438133, 90436754, 90381888, 90435156,
               242695353, 242697061, 242698298, 242700111, 242670122, 242697064, 242698301, 242700112,
               242695586, 242697065, 242698299, 242700115, 341622185, 356128163, 356178983, 356178982,
               356102564, 356178980, 356178981, 356121026, 242670531, 237614750, 237616146, 237617260,
               242277870, 242264268, 242262448, 237617261, 242670532, 242264269, 237616147, 242261051,
               242171299, 242171932, 237616148, 242173734, 237606882, 242264270, 242262449, 242261050,
               386568075, 386568076, 386568077, 386568078, 386568079, 386568080, 386568081, 386568082,
               386568084, 386568085, 386568086, 386568087, 386568088, 386568089, 386568090, 386568091,
               236127733, 236127734, 236127735, 385578749, 219934666, 219936475, 219936476, 385585415,
               219936477, 219936478, 219936479, 385588115, 236127736, 236127737, 236127738, 236127739,
               236127740, 236127741, 411689443, 411695592, 411698852, 411707482, 411710924, 411715897]


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


def sorted_by_current_nmids(items):
    sorted_items = {}
    for item_id in current_ids:
        if item_id in items:
            sorted_items[item_id] = items[item_id]

    # Затем добавляем оставшиеся
    for item_id, value in items.items():
        if item_id not in sorted_items:
            sorted_items[item_id] = value

    return sorted_items


def sorted_by_turnover_total(items: dict, descending: bool = False) -> dict:
    """
    Сортирует словарь items по значению ключа 'turnover_total' во вложенных словарях.

    :param items: словарь, где ключ — артикул, значение — словарь с ключом 'turnover_total'
    :param descending: если True — сортировка по убыванию, иначе — по возрастанию
    :return: новый отсортированный словарь
    """
    sorted_items = dict(
        sorted(
            items.items(),
            key=lambda item: item[1].get('turnover_total', 0),
            reverse=descending
        )
    )
    return sorted_items


def get_filter_by_articles():
    sql_query = """
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
          WHERE рисунок.value IS NOT NULL AND цвет.value IS NOT NULL
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
    conn = connect_to_database()
    with conn.cursor() as cursor:
        cursor.execute(sql_query, )
        rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    dict_rows = [dict(zip(columns, row)) for row in rows]
    data = dict_rows[0]["result"]

    data = sorted(
        [
            {'tail': f"{tail}-{color}", 'nmids': ids} for tail, colors in data.items() for color, ids in colors.items()
        ],
        key=lambda x: x['tail'].lower()
    )
    return data


def abc_classification(data: dict):
    # Шаг 1: Сортируем по количеству заказов (убывание)
    sorted_items = sorted(data.items(), key=lambda x: x[1]["orders"], reverse=True)

    # Шаг 2: Присваиваем категорию
    for i, (art, info) in enumerate(sorted_items):
        vendorcode = info["vendorcode"].lower()
        if ("3240" in vendorcode or "3250" in vendorcode or "4240" in vendorcode or "4250" in vendorcode
                or "11ww" in vendorcode or "33ww" in vendorcode or "55ww" in vendorcode or "66ww" in vendorcode):
            info["ABC"] = "A"
        elif ("3260" in vendorcode or "4260" in vendorcode or "4270" in vendorcode or "44ww" in vendorcode
              or "77ww" in vendorcode or "88ww" in vendorcode):
            info["ABC"] = "B"
        elif "3270" in vendorcode or "22ww" in vendorcode:
            info["ABC"] = "C"

    return dict(sorted_items)


@login_required_cust
def main_view(request):
    return render(request, 'main.html')


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
                p.spp as spp,
                COALESCE(r.is_active, FALSE) AS is_active,
                COALESCE(s.total_quantity, 0) AS quantity
            FROM
                myapp_price p
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
            LEFT JOIN myapp_repricer r ON p.lk_id = r.lk_id AND p.nmid = r.nmid
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
        tail_filter_options = get_filter_by_articles()

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
        if not sort_by:
            dict_rows = {i["nmid"]:i for i in dict_rows}
            dict_rows = sorted_by_current_nmids(dict_rows)
            dict_rows = list(dict_rows.values())

        paginator = Paginator(dict_rows, per_page)
        page_obj = paginator.get_page(page_number)

        # logger.info(f"123123 {page_obj.object_list}")


    except Exception as e:
        logger.error(f"Error in repricer_view: {e}")
        page_obj = []
        nmids = []
        paginator = None

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
    })


@require_POST
@login_required_cust
def repricer_save(request):
    payload = json.loads(request.body)
    items = payload.get('items', [])
    try:
        for item in items:
            if not item["keep_price"].isdigit(): item["keep_price"] = 0
            lk_instance = WbLk.objects.get(id=item['lk_id'])
            Repricer.objects.update_or_create(
                lk=lk_instance,
                nmid=item['nmid'],
                defaults={
                    'keep_price': item['keep_price'],
                    'is_active': item['is_active']
                }
            )
    except Exception as e:
        logger.error(f"Error in repricer_save: {e}")

    return JsonResponse({'status': 'ok', 'received': len(items)})


@login_required_cust
def podsort_view(request):
    now_msk = datetime.now() + timedelta(hours=3)
    yesterday_end = now_msk.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    tree_days_ago = yesterday_end - timedelta(days=3)
    seven_days_ago = yesterday_end - timedelta(days=7)
    two_weeks_ago = yesterday_end - timedelta(weeks=2)
    thirty_days_ago = yesterday_end - timedelta(days=30)

    turnover_periods = [a for a in range(25, 71, 5)]
    order_periods = [3, 7, 14, 30]

    page_sizes = [5, 10, 20, 50, 100]
    nmid_filter = request.GET.getlist('nmid', "")
    warehouse_filter = request.GET.getlist('warehouse', "")
    per_page = int(request.GET.get('per_page', 10))
    page_number = int(request.GET.get('page', 1))

    sort_by = request.GET.get("sort_by", "")  # значение по умолчанию
    order = request.GET.get("order", "")  # asc / desc

    period_ord = int(request.GET.get('period_ord', 14))
    if period_ord == 3:
        period = tree_days_ago
        name_column_available = "s.days_in_stock_last_3"
    elif period_ord == 7:
        period = seven_days_ago
        name_column_available = "s.days_in_stock_last_7"
    elif period_ord == 14:
        period = two_weeks_ago
        name_column_available = "s.days_in_stock_last_14"
    elif period_ord == 30:
        period = thirty_days_ago
        name_column_available = "s.days_in_stock_last_30"
    params = [period]

    turnover_change = int(request.GET.get('turnover_change', 40))

    warehouses = ["Казань", "Подольск", "Коледино", "Тула", "Екатеринбург - Испытателей 14г", "Электросталь",
                  "Краснодар", "Новосибирск", "Санкт-Петербург Уткина Заводь"]
    # Создаём отображение для быстрого доступа к индексу
    warehouse_priority = {name: index for index, name in enumerate(warehouses)}

    if nmid_filter:
        placeholders = ', '.join(['%s'] * len(nmid_filter))
        nmid_query = f"WHERE p.nmid IN ({placeholders})"
        params.extend(nmid_filter)
    else:
        nmid_query = ""

    if warehouse_filter:
        warehouse_s = " OR ".join(f"s.warehousename LIKE '{wh.split()[0]}%%'" for wh in warehouse_filter)
        warehouse_o = " OR ".join(f"o.warehousename LIKE '{wh.split()[0]}%%'" for wh in warehouse_filter)
        warehouse_su = " OR ".join(f"""su."warehouseName" LIKE '{wh.split()[0]}%%'""" for wh in warehouse_filter)
    else:
        warehouse_s = " OR ".join(f"s.warehousename LIKE '{wh.split()[0]}%%'" for wh in warehouses)
        warehouse_o = " OR ".join(f"o.warehousename LIKE '{wh.split()[0]}%%'" for wh in warehouses)
        warehouse_su = " OR ".join(f"""su."warehouseName" LIKE '{wh.split()[0]}%%'""" for wh in warehouses)

    try:
        sql_query = f"""
            WITH
                -- 1) Сумма остатков по складам
                stocks_agg AS (
                    SELECT
                        s.nmid,
                        s.warehousename,
                        {name_column_available} AS available,
                        SUM(s.quantity) AS total_quantity
                    FROM myapp_stocks s
                    WHERE
                        {warehouse_s}
                    GROUP BY
                        s.nmid, s.warehousename, {name_column_available}
                ),
                
                -- 2) Количество заказов по складам (за 2 недели)
                orders_agg AS (
                    SELECT
                        o.nmid,
                        o.warehousename,
                        COUNT(o.id) AS total_orders
                    FROM myapp_orders o
                    WHERE
                        o.date >= %s
                        AND (
                            {warehouse_o}
                        )
                    GROUP BY
                        o.nmid, o.warehousename
                ),
                
                -- 3) Унион всех складов, где были либо остатки, либо заказы
                all_wh AS (
                    SELECT nmid, warehousename FROM stocks_agg
                    UNION
                    SELECT nmid, warehousename FROM orders_agg
                )
                
                -- 4) Основной запрос: все товары + все склады из union-а + подтягиваем агрегаты
                SELECT
                    p.id          AS id,
                    p.nmid        AS nmid,
                    p.vendorcode  AS vendorcode,
                    w.warehousename,
                    COALESCE(sa.total_quantity, 0) AS total_quantity,
                    COALESCE(sa.available, 0)      AS available, 
                    COALESCE(oa.total_orders,   0) AS total_orders,
                    pr.spp   AS spp
                FROM
                    myapp_nmids p
                LEFT JOIN all_wh w
                    ON p.nmid = w.nmid
                LEFT JOIN stocks_agg sa
                    ON p.nmid = sa.nmid
                   AND w.warehousename = sa.warehousename
                LEFT JOIN myapp_price pr
                    ON p.nmid = pr.nmid
                LEFT JOIN myapp_repricer rp
                    ON p.nmid = rp.nmid
                LEFT JOIN orders_agg oa
                    ON p.nmid = oa.nmid
                   AND w.warehousename = oa.warehousename {nmid_query}
                ORDER BY
                    p.nmid,
                    w.warehousename;

        """
        conn = connect_to_database()
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql_query, params)
                rows = cursor.fetchall()
            except Exception as e:
                logger.exception(f"Сбой при выполнении podsort_view. Error: {e}")
            columns = [desc[0] for desc in cursor.description]
            dict_rows = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Чтото с запросом в podsort_view: {e}")

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
    tail_filter_options = get_filter_by_articles()

    try:
        items = {}
        for row in dict_rows:
            if items.get(row["nmid"]):
                items[row["nmid"]]["orders"] += row["total_orders"]
                items[row["nmid"]]["stock"] += row["total_quantity"]
            else:
                items[row["nmid"]] = {
                    "id": row["id"],
                    "article": row["nmid"],
                    "vendorcode": row["vendorcode"],
                    "orders": row["total_orders"],
                    "stock": row["total_quantity"],
                    "ABC": "формула",
                    "turnover_total": 0,
                    "spp": row["spp"],
                    "stock_on_produce": 0,
                    "move_to_rf": "пока пусто",
                    "subitems": []
                }
            if row["warehousename"]:
                items[row["nmid"]]["subitems"].append(
                    {
                        "warehouse": row["warehousename"],
                        "order": row["total_orders"],
                        "stock": row["total_quantity"],
                        "turnover": int(row["total_quantity"] / (row["total_orders"] / period_ord)) if row[
                            "total_orders"] else row["total_quantity"],
                        "rec_delivery": 0,
                        "time_available": row["available"],
                    }
                )

        for key, value in items.items():
            items[key]["turnover_total"] = int(items[key]["stock"] / (items[key]["orders"] / period_ord)) \
                if items[key]["orders"] else items[key]["stock"]
            items[key]["color"] = "green"
            if items[key]["subitems"]:
                items[key]["subitems"].sort(
                    key=lambda x: warehouse_priority.get(x["warehouse"], len(warehouse_priority))
                )

                for index, i in enumerate(items[key]["subitems"]):
                    items[key]["subitems"][index]["rec_delivery"] = int(
                        (items[key]["subitems"][index]["order"] / items[key]["subitems"][index]["time_available"]) * turnover_change - items[key]["subitems"][index]["stock"]
                    ) if items[key]["subitems"][index]["time_available"] else 0

                    if items[key]["subitems"][index]["time_available"] < 5:
                        items[key]["subitems"][index]["rec_delivery"] = int(items[key]["subitems"][index]["rec_delivery"] / 2)

                    if items[key]["subitems"][index]["rec_delivery"] <= -100 or items[key]["subitems"][index]["rec_delivery"] >= 100:
                        items[key]["subitems"][index]["color"] = "red"
                        items[key]["color"] = "red" if items[key]["turnover_total"] < 25 else "white"
                    elif 40 <= items[key]["subitems"][index]["rec_delivery"] < 100 or -40 >= items[key]["subitems"][index]["rec_delivery"] > -100:
                        items[key]["subitems"][index]["color"] = "yellow"
                    elif 0 < items[key]["subitems"][index]["rec_delivery"] < 40 or -1 >= items[key]["subitems"][index]["rec_delivery"] > -40:
                        items[key]["subitems"][index]["color"] = "green"
                    else:
                        items[key]["subitems"][index]["color"] = "white"
        items = abc_classification(items)

        if sort_by == "turnover_total":
            descending = False if order == "asc" else True
            items = sorted_by_turnover_total(items, descending)
        else:
            items = sorted_by_current_nmids(items)

        items = list(items.values())

        # чистим массив у которого пустые вложения по складам
        items = [item for item in items if item["subitems"]]

        paginator = Paginator(items, per_page)
        page_obj = paginator.get_page(page_number)
    except Exception as e:
        logger.error(f"Ошибка при вторичной обработке данных в podsort_view: {e}")
        page_obj = []
        paginator = None

    return render(
        request,
        "podsort.html",
        {
            "warehouses": warehouses,
            "warehouse_filter": warehouse_filter,
            "nmids": combined_list,
            "nmid_filter": nmid_filter,
            "items": page_obj,
            "paginator": paginator,
            "turnover_periods": turnover_periods,
            "order_periods": order_periods,
            "period_ord": period_ord,
            "turnover_change": turnover_change,
            "page_sizes": page_sizes,
            "per_page": per_page,
            "sort_by": sort_by,
            "order": order,
            "tail_filter_options": tail_filter_options,
        }
    )


@require_POST
@login_required_cust
def export_excel(request):
    current_headers = {
        "nmid": "Артикул",
        "vendorcode": "Артикул продавца",
        "redprice": "Красная цена",
        "quantity": "Остаток",
        "keep_price": "Поддерживать цену",
        "spp": "spp",
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
                row = [item.get(col, "") for col in headers]
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
        "Артикул", "Внутренний артикул", "Заказы", "Остатки", "АВС по размерам", "Оборачиваемость общая", "spp", "Остатки на производстве (метр)", "В дороге до РФ (дата)"
    ]
    subheaders = ["Склад", "Заказы", "Остатки", "Оборачиваемость", "Рек. поставка", "Дни в наличии"]

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
        ws.cell(row=row_num, column=3, value=item["orders"])
        ws.cell(row=row_num, column=4, value=item["stock"])
        ws.cell(row=row_num, column=5, value=item["ABC"])
        ws.cell(row=row_num, column=6, value=item["turnover_total"])
        ws.cell(row=row_num, column=7, value=item["spp"])
        ws.cell(row=row_num, column=8, value=item["stock_on_produce"])
        ws.cell(row=row_num, column=9, value=item["move_to_rf"])

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
                ws.cell(row=row_num, column=4, value=subitem["stock"])
                ws.cell(row=row_num, column=5, value=subitem["turnover"])
                ws.cell(row=row_num, column=6, value=subitem["rec_delivery"])
                ws.cell(row=row_num, column=7, value=subitem["time_available"])

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
        logger.info(shipments)
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
        # logger.info(shipments)
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