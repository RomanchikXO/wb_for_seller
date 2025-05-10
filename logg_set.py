LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{asctime} | {levelname} | {name} | {message}',
            'style': '{',  # Используем новый стиль форматирования
        },
    },
    'handlers': {
        'db': {
            'level': 'INFO',  # Уровень логирования для этого обработчика
            'class': 'logging_config.DBLogHandler',  # Путь к нашему кастомному обработчику
            'formatter': 'verbose',  # Указываем форматирование
        },
    },
    'loggers': {
        'django_app': {  # Используем логгер Django
            'handlers': ['db'],  # Добавляем обработчик для записи в базу данных
            'level': 'INFO',  # Уровень логирования
            'propagate': True,  # Пропагируем лог в другие обработчики
        },
        'myapp': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'core': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'database': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'parsers': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'cookie_updater': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'bot': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'google': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
        'mpstat': {
            'handlers': ['db'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}
