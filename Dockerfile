FROM python:3.11-slim

WORKDIR /app

# Изменим источники репозиториев на архивные зеркала
RUN echo "deb http://archive.debian.org/debian/ bookworm main" > /etc/apt/sources.list
RUN echo "deb http://archive.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list

# Установим curl с использованием IPv4
RUN apt-get update -o Acquire::ForceIPv4=true && apt-get install -y curl

# Установим зависимости Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скачиваем и настраиваем wait-for-it
RUN curl -o /wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh \
    && chmod +x /wait-for-it.sh

# Копируем весь код проекта
COPY . .

# По умолчанию ничего не запускаем — это делает docker-compose
CMD ["bash"]
