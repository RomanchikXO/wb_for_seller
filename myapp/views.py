from django.http import HttpResponse
from django.shortcuts import render


def myapp_index(request):
    return HttpResponse("Hello!")