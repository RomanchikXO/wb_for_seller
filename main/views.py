from django.core.paginator import Paginator
from myapp.models import Price
from django.shortcuts import render
from decorators import login_required_cust

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

    custom_data = CustomUser.objects.get(id=request.user.id)
    group_id = custom_data.groups.id

    try:
        queryset = (
            Price.objects
            .filter(lk__groups_id=group_id)
            .prefetch_related('lk__repricer')
            .values('nmid', 'vendorcode', 'redprice', 'lk__repricer__keep_price', 'lk__repricer__is_active')
        )

        # Implement pagination
        paginator = Paginator(queryset, per_page)
        page_obj = paginator.get_page(page_number)

    except Exception as e:
        logger.error(f"Error in repricer_view: {e}")
        page_obj = []
        paginator = None

    return render(request, 'repricer.html', {
        'page_obj': page_obj,
        'per_page': per_page,
        'paginator': paginator,
        'page_sizes': page_sizes,
    })