import contextvars

task_context = contextvars.ContextVar("task_context", default={})
