FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md requirements.txt ./
COPY meshcore_tcp_bot ./meshcore_tcp_bot
COPY config ./config

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir .

EXPOSE 8080

CMD ["python", "-m", "meshcore_tcp_bot", "--config", "/app/config/config.toml"]
