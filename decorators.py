from log_context import task_context
from functools import wraps
from django.shortcuts import redirect

from myapp.models import CustomUser


def with_task_context(task_name):
    def decorator(func):
        @wraps(func)  # <-- сохраняем имя и метаданные
        def wrapper(*args, **kwargs):
            token = task_context.set({'task_name': task_name})
            try:
                return func(*args, **kwargs)
            finally:
                task_context.reset(token)
        return wrapper
    return decorator


def login_required_cust(view_func):
    """
    todo Возможно надо будет удалить блок try except дабы ошибки не выбрасывали на страницу регистрации

    Декоратор сессии
    Args:
        view_func:

    Returns:

    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        try:
            if request.session.get('user_id') and CustomUser.objects.get(id=request.session.get('user_id')) or request.get("export_mode")=="full":
                return view_func(request, *args, **kwargs)
            else:
                return redirect(f"/login/?next={request.path}")
        except:
            return redirect(f"/login/?next={request.path}")
    return wrapper