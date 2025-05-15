from django.core.paginator import Paginator
from django.http import JsonResponse
import json

from myapp.models import Price, Stocks, Repricer, WbLk
from django.shortcuts import render
from decorators import login_required_cust
from django.views.decorators.http import require_POST
from database.DataBase import connect_to_database
from datetime import datetime, timedelta

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
               62999168, 90443541, 90439858, 74512724, 247412508, 247412666, 247412895, 247412942,
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


@login_required_cust
def main_view(request):
    return render(request, 'main.html')


@login_required_cust
def repricer_view(request):
    page_sizes = [5, 10, 20, 50, 100]
    per_page = int(request.GET.get('per_page', 10))
    page_number = int(request.GET.get('page', 1))
    nmid_filter = request.GET.getlist('nmid')  # фильтр по артикулвм
    sort_by = request.GET.get('sort_by', 'quantity')  # значение по умолчанию
    order = request.GET.get('order', 'asc')  # asc / desc

    # Валидные поля сортировки (ключ = название в шаблоне, значение = поле в ORM)
    valid_sort_fields = {
        'redprice': 'redprice',
        'quantity': 'quantity',
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
            else:
                sql_query += """
                        ORDER BY
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

        # logger.info(f"123123 {page_obj.object_list}")


    except Exception as e:
        logger.error(f"Error in repricer_view: {e}")
        page_obj = []
        nmids = []
        paginator = None

    return render(request, 'repricer.html', {
        'page_obj': page_obj,
        'per_page': per_page,
        'paginator': paginator,
        'page_sizes': page_sizes,
        'nmids': combined_list,
        'nmid_filter': nmid_filter,
        'sort_by': sort_by,
        'order': order,
    })


@login_required_cust
@require_POST
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


def abc_classification(data: dict):
    # Шаг 1: Сортируем по количеству заказов (убывание)
    sorted_items = sorted(data.items(), key=lambda x: x[1]["orders"], reverse=True)

    total_count = len(sorted_items)
    a_cutoff = int(total_count * 0.2)
    b_cutoff = int(total_count * 0.5)  # 20% + 30%

    # Шаг 2: Присваиваем категорию
    for i, (art, info) in enumerate(sorted_items):
        if i < a_cutoff:
            info["ABC"] = "A"
        elif i < b_cutoff:
            info["ABC"] = "B"
        else:
            info["ABC"] = "C"

    return dict(sorted_items)


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

    period_ord = int(request.GET.get('period_ord', 14))
    if period_ord == 3:
        period = tree_days_ago
    elif period_ord == 7:
        period = seven_days_ago
    elif period_ord == 14:
        period = two_weeks_ago
    elif period_ord == 30:
        period = thirty_days_ago

    turnover_change = int(request.GET.get('turnover_change', 40))

    warehouses = ["Казань", "Подольск", "Екатеринбург", "Новосибирск", "Краснодар", "Коледино", "Тула",
                  "Санкт-Петербург"]
    try:
        sql_query = """
            WITH
                -- 1) Сумма остатков по складам
                stocks_agg AS (
                    SELECT
                        s.nmid,
                        s.warehousename,
                        SUM(s.quantity) AS total_quantity
                    FROM myapp_stocks s
                    WHERE
                        s.warehousename LIKE 'Казань%%'   OR
                        s.warehousename LIKE 'Подольск%%' OR
                        s.warehousename LIKE 'Екатеринбург%%' OR
                        s.warehousename LIKE 'Новосибирск%%' OR
                        s.warehousename LIKE 'Краснодар%%' OR
                        s.warehousename LIKE 'Коледино%%' OR
                        s.warehousename LIKE 'Тула%%' OR
                        s.warehousename LIKE 'Санкт-Петербург%%'
                    GROUP BY
                        s.nmid, s.warehousename
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
                            o.warehousename LIKE 'Казань%%'   OR
                            o.warehousename LIKE 'Подольск%%' OR
                            o.warehousename LIKE 'Екатеринбург%%' OR
                            o.warehousename LIKE 'Новосибирск%%' OR
                            o.warehousename LIKE 'Краснодар%%' OR
                            o.warehousename LIKE 'Коледино%%' OR
                            o.warehousename LIKE 'Тула%%' OR
                            o.warehousename LIKE 'Санкт-Петербург%%'
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
                
                -- 4) Основной запрос: все товары + все склады из uni­on-а + подтягиваем агрегаты
                SELECT
                    p.id          AS id,
                    p.nmid        AS nmid,
                    p.vendorcode  AS vendorcode,
                    w.warehousename,
                    COALESCE(sa.total_quantity, 0) AS total_quantity,
                    COALESCE(oa.total_orders,   0) AS total_orders
                FROM
                    myapp_nmids p
                LEFT JOIN all_wh w
                    ON p.nmid = w.nmid
                LEFT JOIN stocks_agg sa
                    ON p.nmid = sa.nmid
                   AND w.warehousename = sa.warehousename
                LEFT JOIN orders_agg oa
                    ON p.nmid = oa.nmid
                   AND w.warehousename = oa.warehousename
                ORDER BY
                    p.nmid,
                    w.warehousename;

        """
        conn = connect_to_database()
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql_query, [period])
                rows = cursor.fetchall()
            except Exception:
                logger.exception("Сбой при выполнении podsort_view")
            columns = [desc[0] for desc in cursor.description]
            dict_rows = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Чтото с запросом в podsort_view: {e}")

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
                    }
                )

        for key, value in items.items():
            items[key]["turnover_total"] = int(items[key]["stock"] / (items[key]["orders"] / period_ord)) \
                if items[key]["orders"] else items[key]["stock"]
            if items[key]["subitems"]:
                for index, i in enumerate(items[key]["subitems"]):
                    items[key]["subitems"][index]["rec_delivery"] = int(
                        (turnover_change - items[key]["subitems"][index]["turnover"]) * (
                                    items[key]["subitems"][index]["order"] / period_ord))
                    # items[key]["subitems"][index]["turnover"] = round(
                    #     items[key]["subitems"][index]["stock"] / items[key]["subitems"][index]["order"]
                    # ) if items[key]["subitems"][index]["order"] else items[key]["subitems"][index]["stock"]

        items = abc_classification(items)
        items = sorted_by_current_nmids(items)
        items = items.values()

        # paginator = Paginator(dict_rows, 10)
        # page_obj = paginator.get_page(1)
    except Exception as e:
        logger.error(f"Ошибка при вторичной обработке данных в podsort_view: {e}")

    return render(
        request,
        "podsort.html",
        {
            "items": items,
            "turnover_periods": turnover_periods,
            "order_periods": order_periods,
            "period_ord": period_ord,
            "turnover_change": turnover_change,
        }
    )
