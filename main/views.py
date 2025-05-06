from django.core.paginator import Paginator
from myapp.models import Price, Stocks
from django.shortcuts import render
from decorators import login_required_cust
from django.db.models import OuterRef, Subquery, Sum, IntegerField


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
    nmid_filter = request.GET.getlist('nmid') #фильтр по артикулвм

    custom_data = CustomUser.objects.get(id=request.session.get('user_id'))
    group_id = custom_data.groups.id

    try:
        stocks_subquery = (
            Stocks.objects
            .filter(lk=OuterRef('lk'), nmid=OuterRef('nmid'))
            .values('lk', 'nmid')
            .annotate(total_quantity=Sum('quantity'))
            .values('total_quantity')[:1]
        )

        # Основной запрос
        queryset = (
            Price.objects
            .filter(lk__groups_id=group_id)
            .prefetch_related('lk__repricer')
            .annotate(quantity=Subquery(stocks_subquery, output_field=IntegerField()))
            .values(
                'lk_id',
                'nmid',
                'vendorcode',
                'redprice',
                'lk__repricer__keep_price',
                'lk__repricer__is_active',
                'quantity',
            )
        )
        # Получаем уникальные nmid из базы
        nmids = queryset.values_list('nmid', flat=True).distinct()

        if nmid_filter:
            queryset = queryset.filter(nmid__in=nmid_filter)

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
    })