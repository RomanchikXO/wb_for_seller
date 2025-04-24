FROM python:3.11-slim

WORKDIR /app

# Изменяем источники репозиториев на стабильные зеркала
RUN echo "deb http://ftp.debian.org/debian/ bookworm main" > /etc/apt/sources.list
RUN echo "deb http://ftp.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list

# Устанавливаем wget для загрузки необходимых файлов
RUN apt-get update -o Acquire::ForceIPv4=true && apt-get install -y wget

# Загружаем и настраиваем wait-for-it
RUN wget -O /wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && chmod +x /wait-for-it.sh

# Устанавливаем зависимости Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY . .

# По умолчанию ничего не запускаем — это делает docker-compose
CMD ["bash"]
