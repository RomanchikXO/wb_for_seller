from django.shortcuts import render
from decorators import login_required_cust
from myapp.models import Price, WbLk
from django.core.paginator import Paginator

@login_required_cust
def main_view(request):
    return render(request, 'main.html')


@login_required_cust
def repricer_view(request):
    user = request.user_obj  # получаем объект юзера из нашего кастомного декоратора
    per_page = int(request.GET.get('per_page', 10))
    page_number = request.GET.get('page', 1)

    prices = Price.objects.filter(lk__groups=user.groups).select_related('lk')

    paginator = Paginator(prices, per_page)
    page_obj = paginator.get_page(page_number)

    return render(request, 'repricer.html', {
        'page_obj': page_obj,
        'per_page': per_page,
        'paginator': paginator,
    })
