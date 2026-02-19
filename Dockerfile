FROM python:3.11-slim

WORKDIR /app

# Скачиваем Xray
RUN apt-get update && apt-get install -y wget unzip && \
    wget https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip && \
    unzip Xray-linux-64.zip && \
    mv xray /usr/local/bin/ && \
    chmod +x /usr/local/bin/xray && \
    rm -rf Xray-linux-64.zip *.dat && \
    apt-get clean

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]