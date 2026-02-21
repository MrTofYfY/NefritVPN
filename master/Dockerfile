FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y wget unzip && \
    wget https://github.com/XTLS/Xray-core/releases/download/v1.8.4/Xray-linux-64.zip && \
    unzip Xray-linux-64.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/xray && \
    rm -f Xray-linux-64.zip && \
    apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data

CMD ["python", "main.py"]
