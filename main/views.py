from django.core.paginator import Paginator
from django.http import JsonResponse
import json

from myapp.models import Price, Stocks, Repricer, WbLk
from django.shortcuts import render
from decorators import login_required_cust
from django.db.models import OuterRef, Subquery, Sum, IntegerField, Case, When, BooleanField
from django.views.decorators.http import require_POST


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
        'status': 'lk__repricer__is_active',
    }

    sort_field = valid_sort_fields.get(sort_by)

    try:
        # Подзапрос для получения total_quantity из Stocks
        stocks_subquery = (
            Stocks.objects
            .filter(lk=OuterRef('lk'), nmid=OuterRef('nmid'))
            .values('lk', 'nmid')
            .annotate(total_quantity=Sum('quantity'))
            .values('total_quantity')[:1]
        )

        # Подзапрос для получения keep_price и is_active из Repricer
        repricer_subquery = Repricer.objects.filter(
            lk=OuterRef('lk'),
            nmid=OuterRef('nmid')
        )

        # Основной запрос
        queryset = (
            Price.objects
            .annotate(
                quantity=Subquery(stocks_subquery, output_field=IntegerField()),
                keep_price=Subquery(repricer_subquery.values('keep_price')[:1], output_field=IntegerField()),
                is_active=Subquery(repricer_subquery.values('is_active')[:1], output_field=BooleanField())
            )
            .values(
                'lk_id',
                'nmid',
                'vendorcode',
                'redprice',
                'keep_price',
                'is_active',
                'quantity',
            )
        )
        # Получаем уникальные nmid из базы
        nmids = queryset.values_list('nmid', flat=True).distinct()

        if nmid_filter:
            queryset = queryset.filter(nmid__in=nmid_filter)

        if sort_field:
            if sort_by == 'quantity':
                # помечаем нули, чтобы увести их в конец
                queryset = queryset.annotate(
                    is_zero=Case(When(quantity=0, then=1), default=0, output_field=IntegerField())
                )
                ordering = ['is_zero', ('-quantity' if order == 'asc' else 'quantity')]
            elif sort_by == 'redprice':
                # помечаем NULL, чтобы увести их в конец
                queryset = queryset.annotate(
                    is_null=Case(When(redprice__isnull=True, then=1), default=0, output_field=IntegerField())
                )
                ordering = ['is_null', (f'-redprice' if order == 'desc' else 'redprice')]
            elif sort_by == 'is_active':
                # помечаем неактивные записи (False), чтобы увести их в конец
                queryset = queryset.annotate(
                    is_inactive=Case(When(is_active=False, then=1), default=0, output_field=IntegerField())
                )
                ordering = ['is_inactive', ('-is_active' if order == 'asc' else 'is_active')]
            else:
                prefix = '-' if order == 'desc' else ''
                ordering = [f'{prefix}{sort_field}']

            queryset = queryset.order_by(*ordering)

        paginator = Paginator(queryset, per_page)
        page_obj = paginator.get_page(page_number)


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
        'nmids': nmids,
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