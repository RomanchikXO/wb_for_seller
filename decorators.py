import json

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
    Декоратор сессии. Разрешает доступ, если пользователь залогинен
    или если запрос — экспорт Excel (export_mode=full).
    """

    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        try:
            export_mode_full = False
            if request.method == "GET":
                export_mode_full = request.GET.get("export_mode") == "full"
            elif request.method == "POST":
                try:
                    payload = json.loads(request.body)
                    params = payload.get("params", {})
                    export_mode_full = params.get("export_mode") == "full"
                except Exception:
                    export_mode_full = False

            if (request.session.get('user_id') and CustomUser.objects.get(id=request.session.get('user_id'))
                    or export_mode_full):
                return view_func(request, *args, **kwargs)
            else:
                return redirect(f"/login/?next={request.path}")
        except:
            return redirect(f"/login/?next={request.path}")
    return wrapper