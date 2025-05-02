from log_context import task_context
from functools import wraps

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
