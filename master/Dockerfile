FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей и Xray
RUN apt-get update && \
    apt-get install -y wget unzip curl && \
    wget https://github.com/XTLS/Xray-core/releases/download/v1.8.4/Xray-linux-64.zip && \
    unzip Xray-linux-64.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/xray && \
    rm -f Xray-linux-64.zip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Создаём папку для данных
RUN mkdir -p /app/data

CMD ["python", "main.py"]
