FROM python:3.11-slim

# FIX: --no-cache-dir экономит ~30MB, --no-install-recommends в apt то же.
# Никаких build tools — aiohttp и websockets имеют manylinux wheels.
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY seoul_relay.py .

# Healthcheck: TCP-коннект к собственному порту. Если процесс умер /
# зависел — Docker рестартует контейнер по restart policy.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import socket,os; s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1', int(os.environ.get('RELAY_PORT','8765')))); s.close()" \
    || exit 1

# -u: unbuffered stdout, важно чтобы print'ы из poll-loop'а попадали в docker logs мгновенно.
ENTRYPOINT ["python", "-u", "seoul_relay.py"]
