FROM python:3.11-slim

WORKDIR /app


# Устанавливаем зависимости Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps
# Копируем весь код проекта
COPY . .

# По умолчанию ничего не запускаем — это делает docker-compose
CMD ["bash"]
