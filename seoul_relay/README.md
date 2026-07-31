# Seoul edge-нода (relay)

Лёгкий standalone-сервис, который запускается на VPS в Сеуле/Токио
рядом с биржами Bithumb и Upbit. Поллит их notices-API с локальным
~5-30мс RTT и пушит свежие листинги в WS-канал, на который подписывается
main-парсер (parser_listing.py) через `api/seoul_relay_receiver.py`.

**Что выигрывает:** −100..−150мс на каждом Korean листинге vs прямой
поллинг из Singapore (где RTT 120мс одна сторона).

## Quick start

### 1. Подготовка VPS

Минимальные требования: 1 vCPU, 512MB RAM, Python 3.11+. Любой Vultr/Hetzner
инстанс в Tokyo (ap-northeast-1) или Seoul за $5/мес подойдёт.

```bash
ssh root@seoul-node
apt update && apt install -y python3.11 python3-pip
git clone https://github.com/yourname/parsers.git
cd parsers/seoul_relay
pip3 install -r requirements.txt
```

### 2. Конфиг

Сгенерируй случайный secret и положи в systemd-юнит:

```bash
export RELAY_SECRET=$(openssl rand -hex 32)
echo "secret = $RELAY_SECRET"
```

### 3. Запуск (тест)

```bash
RELAY_SECRET=<secret> RELAY_PORT=8765 python3 seoul_relay.py
```

В другом терминале проверь:

```bash
# auth должен fail
wscat -c "ws://seoul-node-ip:8765/relay?key=wrong"

# auth ok — должен прийти {"type":"hello",...}
wscat -c "ws://seoul-node-ip:8765/relay?key=$RELAY_SECRET"
```

### 4. systemd unit

`/etc/systemd/system/seoul-relay.service`:

```ini
[Unit]
Description=Seoul edge relay for Bithumb/Upbit listing announcements
After=network-online.target

[Service]
Type=simple
User=relay
WorkingDirectory=/opt/seoul_relay
Environment="RELAY_PORT=8765"
Environment="RELAY_SECRET=<секрет>"
Environment="BITHUMB_POLL_MS=50"
Environment="UPBIT_POLL_MS=80"
ExecStart=/usr/bin/python3 /opt/seoul_relay/seoul_relay.py
Restart=always
RestartSec=3
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now seoul-relay
journalctl -u seoul-relay -f
```

### 5. TLS (опционально, рекомендуется)

Сам скрипт слушает plain `ws://`. В проде поставь nginx/caddy перед ним:

```nginx
# /etc/nginx/sites-available/seoul-relay
server {
    listen 443 ssl http2;
    server_name seoul.yourdomain.com;
    ssl_certificate /etc/letsencrypt/live/seoul.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/seoul.yourdomain.com/privkey.pem;

    location /relay {
        proxy_pass http://127.0.0.1:8765/relay;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400s;
    }
}
```

Или Caddy (проще):
```
seoul.yourdomain.com {
    reverse_proxy /relay localhost:8765
}
```

### 6. Подписка main-парсера

В `.env` SG-сервера:

```
SEOUL_RELAY_URL=wss://seoul.yourdomain.com/relay?key=<тот же RELAY_SECRET>
SEOUL_RELAY_KEY=<тот же RELAY_SECRET>
```

Перезапусти `parser_listing.py` — в логах должно появиться:
```
[PARSER] Seoul relay listener подключён → wss://seoul.yourdomain.com/relay
```

И в `source_stats.json` через сутки будут видны wins источника
`SEOUL-RELAY-BITHUMB` / `SEOUL-RELAY-UPBIT`.

## Архитектура

```
┌─────────────────┐         ┌────────────────┐
│ Seoul VPS       │         │ Main SG server │
│                 │         │                │
│ ┌─────────────┐ │  WSS    │ ┌────────────┐ │
│ │seoul_relay  │ │◄────────┤ │seoul_relay_│ │
│ │ - poll BTH  │ │  ~100мс │ │receiver.py │ │
│ │ - poll UPB  │ │  one-way│ └─────┬──────┘ │
│ │ - extract   │ │         │       │        │
│ │ - broadcast │ │         │       ▼        │
│ └──────┬──────┘ │         │ process_signal │
│        │~5мс    │         │       │        │
│        ▼        │         │       ▼        │
└────────┴────────┘         │  market_open   │
         │                  │       │        │
         ▼                  │       ▼        │
   api.bithumb.com          │  Bybit/Gate    │
   api.upbit.com            └────────────────┘
```

## Производительность

С `BITHUMB_POLL_MS=50` поллер делает ~20 req/s — это в пределах
публичного rate-limit Bithumb (10 req/s обычно мягкий, на API без auth
часто выше). На случай 429 — backoff 2 секунды.

При burst-листингах (несколько монет в одном `count=20` ответе) все они
улетают в `_broadcast` одним await.

## Стоимость

- Vultr Tokyo Regular: $6/мес (1 vCPU, 1GB RAM) — с запасом
- Hetzner Tokyo CX11: €3.79/мес — самый дешёвый
- DigitalOcean Singapore→nearest = тоже OK но 30мс лишних

## Что не делает (намеренно)

- Не парсит CoinListing/TOA WS — это делает main-парсер (там общий L1/L2 дедуп)
- Не размещает ордера — relay не имеет ключей бирж
- Не делает Twitter / TG — это main-парсер
- Не HTTP/2 — websockets и aiohttp по default HTTP/1.1; для notice-API
  HTTP/2 multiplexing не даёт win
