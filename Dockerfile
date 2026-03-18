FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY meshcore_bot ./meshcore_bot
COPY config ./config

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

CMD ["meshcore_bot", "show-config"]
