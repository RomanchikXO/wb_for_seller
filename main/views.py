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
        dict_rows = []
        paginator = None

    return render(request, 'repricer.html', {
        'page_obj': page_obj,
        'per_page': str(per_page),
        'paginator': paginator,
        'page_sizes': page_sizes,
        'nmids': [str(i["nmid"]) for i in dict_rows],
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