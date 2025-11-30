# Установка Docker
sudo apt install -y docker.io

# Добавить Docker в автозапуск и запустить
sudo systemctl enable docker
sudo systemctl start docker

# Установка Docker Compose (если нужно)
sudo apt install -y docker-compose

# клонируем проект , переходим в него
# не забываем создать .env файл и credentials.json

# запускаем
docker-compose build
docker-compose up -d

# (собираем статик файлы )
docker compose exec django python manage.py collectstatic --noinput

# при изменениях в tasks/ parsers/ tasks.py funcs_db.py celery_config.py mpstat.py logging_config.py logg_set.py
# автоматически на сервере celery_worker перезапускается

# при изменениях в django_app/ logg_set.py
# автоматически на сервере django_app перезапускается

# Удачи!