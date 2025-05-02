import logging
from log_context import task_context

class ContextLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        context = task_context.get({})
        return f"{msg}", kwargs

