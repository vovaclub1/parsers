# FIX: Python 3.9 EOL 2025-10, плюс код использует PEP 604 синтаксис (X | Y)
# в runtime-аннотациях. Переходим на 3.11-slim — стабильный и компактный.
FROM python:3.11-slim

WORKDIR /Parsers

# FIX: системные зависимости только если нужны (для tgcrypto / cryptography).
# build-essential нужен для компиляции некоторых wheels на slim-образе.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# FIX: --no-cache-dir уменьшает размер образа; pip upgrade чтобы избежать warnings.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/Parsers \
    PYTHONUNBUFFERED=1

# FIX: команду по умолчанию задаём через docker-compose, чтобы один Dockerfile
# обслуживал и delist, и listing контейнеры.
CMD ["python", "-u", "parsers/parser_delist.py"]
