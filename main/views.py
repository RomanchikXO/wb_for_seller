from django.shortcuts import render
from decorators import login_required_cust
from myapp.models import Price, WbLk, CustomUser
from django.core.paginator import Paginator

@login_required_cust
def main_view(request):
    return render(request, 'main.html')


@login_required_cust
def repricer_view(request):
    page_sizes = [5, 10, 20, 50, 100]
    user_groups = CustomUser.objects.all()  # получаем объект юзера из нашего кастомного декоратора
    group_ids = user_groups.values_list('id', flat=True)
    per_page = int(request.GET.get('per_page', 10))
    page_number = request.GET.get('page', 1)

    prices = Price.objects.filter(lk__groups__in=group_ids).order_by('id')

    paginator = Paginator(prices, per_page)
    page_obj = paginator.get_page(page_number)

    return render(request, 'repricer.html', {
        'page_obj': page_obj,
        'per_page': per_page,
        'paginator': paginator,
        'page_sizes': page_sizes,
    })
