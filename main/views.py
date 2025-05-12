from django.core.paginator import Paginator
from django.http import JsonResponse
import json

from myapp.models import Price, Stocks, Repricer, WbLk
from django.shortcuts import render
from decorators import login_required_cust
from django.db.models import OuterRef, Subquery, Sum, IntegerField, Case, When, BooleanField
from django.views.decorators.http import require_POST
from database.DataBase import connect_to_database


import logging
from context_logger import ContextLogger
from myapp.models import CustomUser

logger = ContextLogger(logging.getLogger("parsers"))

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
            cursor.execute(sql_nmid,)
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
            if not item["keep_price"].isdigit(): item["keep_price"]=0
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
    turnover_periods = [a for a in range(25, 71, 5)]
    order_periods = [3, 7, 14, 30]
    warehouses = ["Казань", "Подольск", "Екатеринбург", "Новосибирск", "Краснодар", "Коледино", "Тула", "Санкт-Петербург"]
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
                        s.warehousename LIKE 'Казань%'   OR
                        s.warehousename LIKE 'Подольск%' OR
                        s.warehousename LIKE 'Екатеринбург%' OR
                        s.warehousename LIKE 'Новосибирск%' OR
                        s.warehousename LIKE 'Краснодар%' OR
                        s.warehousename LIKE 'Коледино%' OR
                        s.warehousename LIKE 'Тула%' OR
                        s.warehousename LIKE 'Санкт-Петербург%'
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
                        o.date >= NOW() - INTERVAL '2 weeks'
                        AND (
                            o.warehousename LIKE 'Казань%'   OR
                            o.warehousename LIKE 'Подольск%' OR
                            o.warehousename LIKE 'Екатеринбург%' OR
                            o.warehousename LIKE 'Новосибирск%' OR
                            o.warehousename LIKE 'Краснодар%' OR
                            o.warehousename LIKE 'Коледино%' OR
                            o.warehousename LIKE 'Тула%' OR
                            o.warehousename LIKE 'Санкт-Петербург%'
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
            cursor.execute(sql_query,)
            rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        dict_rows = [dict(zip(columns, row)) for row in rows]

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
                        "turnover": 0
                    }
                )

        for key, value in items.items():
            items[key]["turnover_total"] = round(items[key]["stock"] / items[key]["orders"]) \
                if items[key]["orders"] else items[key]["stock"]
            if items[key]["subitems"]:
                for index, i in enumerate(items[key]["subitems"]):
                    items[key]["subitems"][index]["turnover"] = round(
                        items[key]["subitems"][index]["stock"] / items[key]["subitems"][index]["order"]
                    ) if items[key]["subitems"][index]["order"] else items[key]["subitems"][index]["stock"]

        items = items.values()


        # paginator = Paginator(dict_rows, 10)
        # page_obj = paginator.get_page(1)
    except Exception as e:
        logger.error(f"Error in podsort_view: {e}")




    # items = [
    #     {
    #         "id": 1,
    #         "article": "12345",
    #         "vendorcode": "A123",
    #         "orders": 10,
    #         "stock": 50,
    #         "ABC": "формула",
    #         "turnover_total": "остатки / кол-во заказов",
    #         "subitems": [
    #             {"warehouse": "КазанПлова", "order": 7, "stock": 20, "turnover": "20/7"},
    #             {"warehouse": "ЕкатеринБургер", "order": 3, "stock": 30, "turnover": "30/3"},
    #         ],
    #     },
    #     {
    #         "id": 2,
    #         "article": "67890",
    #         "internal_article": "B456",
    #         "orders": 20,
    #         "warehouse": "Склад 2",
    #         "stock": 30,
    #         "subitems": [
    #             {"sub_article": "67890-1", "order": 8, "stock": 12},
    #             {"sub_article": "67890-2", "order": 5, "stock": 8},
    #         ],
    #     },
    # ]

    return render(
        request,
        "podsort.html",
        {
            "items": items,
            "turnover_periods": turnover_periods,
            "order_periods": order_periods,
        }
    )

