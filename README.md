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
docker-compose up --build -d

# если надо остановить
docker-compose down

# если я сделал git pull и не менял Dockerfile или requirements.txt
docker-compose up -d 
# иначе
docker-compose down
docker-compose up --build -d