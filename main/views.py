from django.shortcuts import render
from decorators import login_required_cust


@login_required_cust
def main_view(request):
    return render(request, 'main.html')


@login_required_cust
def repricer_view(request):
    return render(request, 'repricer.html')
