#!/usr/bin/env python3
"""
seoul_relay.py — standalone Seoul edge-node.

Цель: запустить на отдельной VPS в Корее (Vultr Seoul / Hetzner Tokyo)
рядом с биржами. Поллит api.bithumb.com и api.upbit.com с локальным
~5-30мс RTT и пушит новые нотисы в WS-канал.

Main-парсер на SG/SGT подключается к этому WS-каналу как `seoul_relay_receiver`
и получает сигнал за ~85-120мс KR→SG one-way вместо ~240мс RTT при прямом
поллинге.

Запуск:
    export RELAY_PORT=8765
    export RELAY_SECRET=<длинная случайная строка>
    python3 seoul_relay.py

Безопасность: TLS terminating на nginx/caddy перед этим WS (см. README.md
рядом с этим файлом). Сам скрипт слушает plain ws://0.0.0.0:RELAY_PORT,
аутентификация — `?key=RELAY_SECRET` в query.

Зависимости: см. requirements.txt. Минимум: aiohttp, websockets.
"""
from __future__ import annotations

import asyncio
import hmac  # FIX-SEC: constant-time secret compare в _ws_handler
import json
import os
import re
import sys
import time
from typing import Set
from urllib.parse import parse_qs, urlparse

import aiohttp
import websockets

# FIX: curl_cffi — для Upbit нужна имитация TLS handshake Chrome (JA3
# fingerprint). aiohttp/обычный curl с datacenter IP в Корее CF режет 403.
# curl_cffi.AsyncSession(impersonate="chrome124") даёт реальный Chrome TLS
# hello → CF принимает запрос. Bithumb продолжаем качать через aiohttp.
try:
    from curl_cffi.requests import AsyncSession as _CurlAsyncSession
    HAS_CURL_CFFI = True
except ImportError:
    _CurlAsyncSession = None  # type: ignore[assignment,misc]
    HAS_CURL_CFFI = False

# orjson на сырых байтах быстрее aiohttp r.json() (stdlib json + проверка
# content-type/charset). На Bithumb-нотисах (~5-10KB, poll каждые 50мс) и
# на market/all это снимает заметный кусок parse-времени.
try:
    import orjson as _orjson  # type: ignore[import-not-found]
    def _loads(b):
        return _orjson.loads(b)
    def _dumps(o) -> str:
        return _orjson.dumps(o).decode()
except ImportError:
    def _loads(b):
        return json.loads(b)
    def _dumps(o) -> str:
        return json.dumps(o, separators=(",", ":"))


async def _aio_json(r):
    """Парсит aiohttp-ответ через orjson по сырым байтам."""
    return _loads(await r.read())

# ── Конфиг ────────────────────────────────────────────────────────
PORT          = int(os.environ.get("RELAY_PORT", "8765"))
SECRET        = os.environ.get("RELAY_SECRET", "")
BITHUMB_POLL_MS = int(os.environ.get("BITHUMB_POLL_MS", "50"))
# FIX: Upbit api-manager имеет публичный rate-limit 10 req/s per IP.
# При 80мс interval'е → 12.5 req/s = выше лимита, 429 в логи.
# 150мс = 6.6 req/s per session, на 3 прокси suma ≈ 20 req/s но
# распределяется round-robin → каждый прокси видит свои 6.6 req/s.
UPBIT_POLL_MS   = int(os.environ.get("UPBIT_POLL_MS", "150"))
# FIX: Cloudflare на Korean datacenter IP блочит Upbit even single-endpoint.
# Если browser-like headers не пробивают — оставь UPBIT_ENABLED=0 в .env,
# Upbit-сигналы продолжит ловить main-парсер на SG (где CF мягче).
# В этом случае Seoul relay качает только Bithumb (что и так даёт −100мс
# на Bithumb-листингах). Bithumb работает без проблем — у него своё API.
UPBIT_ENABLED   = os.environ.get("UPBIT_ENABLED", "1").lower() in ("1", "true", "yes", "on")

if not SECRET:
    print("[FATAL] RELAY_SECRET не задан — отказ старта (без аутентификации запускать нельзя)", file=sys.stderr)
    sys.exit(1)

if len(SECRET) < 16:
    print(f"[WARN] RELAY_SECRET слишком короткий ({len(SECRET)} символов) — лучше ≥32", file=sys.stderr)

# Market-diff триггер: ловим новый ТОРГУЕМЫЙ рынок в момент появления в
# /v1/market/all — это часто РАНЬШЕ (или одновременно) нотиса. Downstream
# L2-дедуп по монете гарантирует, что market-сигнал и notice-сигнал не
# откроют позицию дважды. Чисто аддитивный источник: если эндпоинт упадёт/
# забанится — основной notice-поллинг не страдает.
MARKET_DIFF_ENABLED = os.environ.get("MARKET_DIFF_ENABLED", "1").lower() in ("1", "true", "yes", "on")
MARKET_POLL_MS  = int(os.environ.get("MARKET_POLL_MS", "200"))
UPBIT_MARKET_URL   = "https://api.upbit.com/v1/market/all"
BITHUMB_MARKET_URL = "https://api.bithumb.com/v1/market/all?isDetails=false"

BITHUMB_NOTICES_URL = "https://api.bithumb.com/v1/notices?count=20"
UPBIT_NOTICES_URL_TEMPLATE = "https://api-manager.upbit.com/api/v1/announcements/{id}"

# FIX: list-endpoint (?page=...) Upbit защищён Cloudflare и отдаёт 403
# на datacenter IP (особенно на свежих Korean IP, где CF строже).
# Single-endpoint /announcements/{id} НЕ блокирован — используем binary
# search для discovery текущего max_id (см. main parser_listing.py).
#
# Baseline на 2026-06-01: actual max=6262 (подтверждено user-side).
# Дискавери начнёт с 6260 и через ~5-10 запросов найдёт реальный max
# через экспоненциальный рост + бинарный поиск.
UPBIT_DISCOVERY_BASELINE = int(os.environ.get("UPBIT_DISCOVERY_BASELINE", "6260"))

# FIX: версия Chrome для curl_cffi impersonate. Если CF введёт новый
# fingerprint check — можно поменять через env без рестарта imagа.
UPBIT_IMPERSONATE = os.environ.get("UPBIT_IMPERSONATE", "chrome124")

# FIX: Korean residential proxies для обхода CF IP-блокировки datacenter
# диапазонов. Формат — CSV строк через запятую:
#   socks5://user:pass@host:port,socks5://user2:pass2@host2:port2
# или для http прокси:
#   http://user:pass@host:port
# или для авторизации по IP-whitelist:
#   socks5://host:port
# Запросы round-robin'ятся между сессиями — каждый прокси видит
# ~80/N мс между запросами, что не вызывает rate-limit.
UPBIT_PROXIES_RAW = os.environ.get("UPBIT_PROXIES", "")

# FIX: для диагностики прокси/CF — первые N не-успешных Upbit-ответов
# логируются с status code и preview. Дальше silent чтобы не флудить.
_UPBIT_ERR_LOG_LIMIT = int(os.environ.get("UPBIT_ERR_LOG_LIMIT", "8"))
_upbit_err_count = 0

# FIX: счётчик 429 для адаптивного backoff. Когда Upbit rate-limit'ит,
# дальнейшие запросы только усугубляют. После N подряд 429 уходим в
# длинный sleep, чтобы лимит сбросился.
_upbit_429_streak = 0
_UPBIT_429_THRESHOLD = int(os.environ.get("UPBIT_429_THRESHOLD", "3"))
_UPBIT_429_COOLDOWN_S = float(os.environ.get("UPBIT_429_COOLDOWN_S", "30"))


def _parse_upbit_proxies(raw: str) -> list[str]:
    """
    Парсит CSV прокси. Поддерживает форматы:
      http://user:pass@host:port      — стандарт
      socks5://user:pass@host:port    — SOCKS
      user:pass@host:port             — без схемы → http://
      host:port:user:pass             — Webshare/IPRoyal стандарт
      user:pass:host:port             — некоторые провайдеры
      host:port                       — без auth
    Все варианты нормализуются в `{scheme}://user:pass@host:port`.
    """
    out: list[str] = []
    for p in (raw or "").split(","):
        p = p.strip()
        if not p:
            continue

        # Извлечь схему если есть
        scheme = "http"
        rest = p
        if "://" in rest:
            scheme, rest = rest.split("://", 1)

        # Уже user:pass@host:port → готово
        if "@" in rest:
            out.append(f"{scheme}://{rest}")
            continue

        parts = rest.split(":")
        if len(parts) == 4:
            # host:port:user:pass (Webshare/IPRoyal) — parts[1] = port (число)
            if parts[1].isdigit():
                host, port, user, pwd = parts
            # user:pass:host:port — parts[3] = port (число)
            elif parts[3].isdigit():
                user, pwd, host, port = parts
            else:
                print(f"[PROXY WARN] непонятный формат '{p[:40]}' — skip", flush=True)
                continue
            out.append(f"{scheme}://{user}:{pwd}@{host}:{port}")
            continue

        if len(parts) == 2 and parts[1].isdigit():
            # host:port без auth
            out.append(f"{scheme}://{rest}")
            continue

        print(f"[PROXY WARN] непонятный формат '{p[:40]}' — skip", flush=True)
    return out

# ── Корейские стоп-слова и категории (зеркало parser_listing.py) ─
_BITHUMB_NOTICE_NEG_CATEGORIES = {
    "거래지원종료", "입출금", "안내", "점검", "이벤트",
}
_BITHUMB_NOTICE_NEG_KEYWORDS = (
    "종료", "중단", "유의", "상장폐지", "해제", "연기", "일시", "점검", "재개",
)
_BITHUMB_NOTICE_TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9]{1,9})\)")
_BITHUMB_NOTICE_ID_RE = re.compile(r"/notice/(\d+)")
_BITHUMB_NOTICE_BANNED = {
    "KRW", "BTC", "ETH", "USD", "USDT", "USDC", "BUSD", "DAI",
    "NFT", "KST", "UTC", "API", "VIP",
    "MARKET", "MARKETS", "LIST",
    "UPBIT", "BITHUMB", "BINANCE", "COINBASE",
}

_UPBIT_NOTICE_TRADE_CATEGORIES = {"거래"}
_UPBIT_NOTICE_NEG_KEYWORDS = (
    "거래 종료", "거래종료", "상장 폐지", "상장폐지", "유의 종목", "유의종목",
    "거래 유의", "거래유의", "거래 지원 종료", "지원 종료",
    "점검", "일시", "연기", "주의",
)
_UPBIT_NOTICE_TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9]{1,9})\)")
_UPBIT_NOTICE_BANNED = _BITHUMB_NOTICE_BANNED


def _extract_bithumb_tickers(title: str, categories: list) -> list[str]:
    for cat in categories:
        if cat in _BITHUMB_NOTICE_NEG_CATEGORIES:
            return []
    for kw in _BITHUMB_NOTICE_NEG_KEYWORDS:
        if kw in title:
            return []
    found: list[str] = []
    seen: set[str] = set()
    for tok in _BITHUMB_NOTICE_TICKER_RE.findall(title):
        token = tok.upper()
        if token in _BITHUMB_NOTICE_BANNED or token in seen:
            continue
        seen.add(token)
        found.append(token)
    return found


def _extract_upbit_tickers(title: str, category: str) -> list[str]:
    if category not in _UPBIT_NOTICE_TRADE_CATEGORIES:
        return []
    for kw in _UPBIT_NOTICE_NEG_KEYWORDS:
        if kw in title:
            return []
    found: list[str] = []
    seen: set[str] = set()
    for tok in _UPBIT_NOTICE_TICKER_RE.findall(title):
        token = tok.upper()
        if token in _UPBIT_NOTICE_BANNED or token in seen:
            continue
        seen.add(token)
        found.append(token)
    return found


# ── Клиенты ───────────────────────────────────────────────────────
_clients: Set[websockets.WebSocketServerProtocol] = set()


async def _broadcast(payload: dict) -> None:
    """Шлёт payload всем подключённым клиентам. Сбойные коннекты выкидываем.

    FIX-RACE: list(_clients) — снапшот ссылок ДО итерации. Без него если
    параллельно отрабатывает _ws_handler.connect/disconnect или другой
    _broadcast (Bithumb+Upbit одновременно) — set.add/discard во время
    `for ws in _clients` бросает 'Set changed size during iteration' и
    корутина падает → листинг НЕ доедет до подписчиков.
    """
    if not _clients:
        return
    msg = _dumps(payload)
    dead = []
    for ws in list(_clients):
        try:
            await ws.send(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _clients.discard(ws)


def _log(tag: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}][{tag}] {msg}", flush=True)


# ── WS-handler ────────────────────────────────────────────────────
async def _ws_handler(ws: websockets.WebSocketServerProtocol) -> None:
    # Аутентификация по query param ?key=...
    try:
        path = ws.request.path  # websockets ≥12
    except Exception:
        path = getattr(ws, "path", "/")
    qs = parse_qs(urlparse(path).query)
    key = (qs.get("key") or [""])[0]
    # FIX-SEC (CRITICAL): hmac.compare_digest вместо '!=' — защита от
    # timing-side-channel'а. Обычное сравнение строк возвращается
    # быстрее на коротком префиксе → атакующий через миллионы коннектов
    # может посимвольно подобрать SECRET.
    if not hmac.compare_digest(key, SECRET):
        _log("AUTH", f"REJECT {ws.remote_address} key_mismatch")
        await ws.close(code=4001, reason="auth failed")
        return

    _clients.add(ws)
    _log("WS", f"CONNECT {ws.remote_address} | clients={len(_clients)}")
    try:
        # Шлём приветствие чтобы клиент знал что коннект жив.
        await ws.send(json.dumps({"type": "hello", "t_ms": int(time.time() * 1000)}))
        # Просто ждём закрытия. Любые сообщения от клиента (ping и т.п.)
        # тихо игнорируем — relay односторонний.
        async for _ in ws:
            pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        _clients.discard(ws)
        _log("WS", f"DISCONNECT {ws.remote_address} | clients={len(_clients)}")


# ── Bithumb poller ────────────────────────────────────────────────
async def _poll_bithumb(http: aiohttp.ClientSession) -> None:
    seen_ids: set[str] = set()

    # Инициализация: помечаем существующие нотисы как уже виденные.
    try:
        async with http.get(BITHUMB_NOTICES_URL, timeout=aiohttp.ClientTimeout(total=4)) as r:
            r.raise_for_status()
            items = await _aio_json(r)
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                m = _BITHUMB_NOTICE_ID_RE.search(item.get("pc_url", "") or "")
                if m:
                    seen_ids.add(m.group(1))
        _log("BITHUMB", f"init: {len(seen_ids)} нотисов в seen")
    except Exception as e:
        _log("BITHUMB", f"init failed: {e!r}")

    interval = BITHUMB_POLL_MS / 1000.0
    _log("BITHUMB", f"poll старт ({BITHUMB_POLL_MS}мс)")

    while True:
        await asyncio.sleep(interval)
        t_send = time.perf_counter()
        try:
            async with http.get(BITHUMB_NOTICES_URL, timeout=aiohttp.ClientTimeout(total=2)) as r:
                r.raise_for_status()
                items = await _aio_json(r)
        except Exception as e:
            _log("BITHUMB", f"poll err: {e!r}")
            await asyncio.sleep(2.0)  # backoff
            continue
        t_recv = time.perf_counter()
        fetch_ms = (t_recv - t_send) * 1000

        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            url = item.get("pc_url", "") or ""
            m = _BITHUMB_NOTICE_ID_RE.search(url)
            if not m:
                continue
            notice_id = m.group(1)
            if notice_id in seen_ids:
                continue
            seen_ids.add(notice_id)

            title = item.get("title") or ""
            categories = item.get("categories") or []
            if not isinstance(categories, list):
                categories = []
            tickers = _extract_bithumb_tickers(title, categories)
            if not tickers:
                _log("BITHUMB", f"skip {notice_id} [{','.join(categories)}] | {title[:60]}")
                continue

            payload = {
                "type": "listing",
                "source": "SEOUL-RELAY-BITHUMB",
                "coins": tickers,
                "title": title,
                "notice_id": int(notice_id),
                "fetch_ms": round(fetch_ms, 1),
                "t_pub_ms": int(time.time() * 1000),
            }
            _log("BITHUMB", f"LISTING {tickers} id={notice_id} ({fetch_ms:.0f}мс) → broadcast({len(_clients)})")
            await _broadcast(payload)


# ── Upbit poller (через curl_cffi) ────────────────────────────────
# Подпись http: используем "Any" так как curl_cffi.AsyncSession — отдельный
# тип, не наследует aiohttp. Все вызовы внутри однотипные (.get → Response
# с .status_code и .json()).
async def _try_fetch_upbit_announcement(
    http, ann_id: int,
) -> tuple[str | None, str]:
    """
    Возвращает (title, category) если id существует и это листинг,
    или (None, "") если 404 / 403 / парс не удался.

    Использует curl_cffi.AsyncSession — она имитирует Chrome TLS handshake,
    что пробивает Cloudflare bot-detection на Korean datacenter IP.
    """
    global _upbit_err_count, _upbit_429_streak
    url = UPBIT_NOTICES_URL_TEMPLATE.format(id=ann_id)
    try:
        # curl_cffi: .get возвращает Response сразу (не async ctx manager).
        # .status_code как у requests; .json() синхронный.
        r = await http.get(url, timeout=2)
        if r.status_code == 404:
            _upbit_429_streak = 0  # 404 — нормальная reply, лимит сброшен
            return None, ""
        if r.status_code == 429:
            _upbit_429_streak += 1
            if _upbit_err_count < _UPBIT_ERR_LOG_LIMIT:
                _upbit_err_count += 1
                _log("UPBIT", f"id={ann_id} 429 (rate-limit) streak={_upbit_429_streak}")
            if _upbit_429_streak >= _UPBIT_429_THRESHOLD:
                _log("UPBIT", f"{_upbit_429_streak} подряд 429 → cooldown {_UPBIT_429_COOLDOWN_S:.0f}с")
                await asyncio.sleep(_UPBIT_429_COOLDOWN_S)
                _upbit_429_streak = 0
                _upbit_err_count = 0  # переоткрываем диагностику после cooldown
            return None, ""
        if r.status_code != 200:
            if _upbit_err_count < _UPBIT_ERR_LOG_LIMIT:
                _upbit_err_count += 1
                preview = r.text[:100].replace("\n", " ")
                _log("UPBIT", f"id={ann_id} status={r.status_code} size={len(r.content)} preview={preview}")
            return None, ""
        # 200 OK — сбрасываем 429-streak
        _upbit_429_streak = 0
        data = r.json()
    except Exception as e:
        if _upbit_err_count < _UPBIT_ERR_LOG_LIMIT:
            _upbit_err_count += 1
            _log("UPBIT", f"id={ann_id} ERR {type(e).__name__}: {e}")
        return None, ""
    if not isinstance(data, dict) or not data.get("success"):
        if _upbit_err_count < _UPBIT_ERR_LOG_LIMIT:
            _upbit_err_count += 1
            _log("UPBIT", f"id={ann_id} success=false data={str(data)[:120]}")
        return None, ""
    body = data.get("data") or {}
    title = body.get("title") or ""
    category = body.get("category") or ""
    return title, category


async def _upbit_id_exists(http, ann_id: int) -> bool:
    """True если /announcements/{ann_id} вернул success=true."""
    title, _ = await _try_fetch_upbit_announcement(http, ann_id)
    return title is not None


async def _discover_upbit_max_id_via_sessions(sessions: list, cycler) -> int:
    """Wrapper над _discover_upbit_max_id с round-robin сессии на каждый probe."""
    # На время discovery просто берём первую сессию (10-25 запросов под одним
    # IP — это OK, CF не банит за нормальную нагрузку). Можно было бы и
    # rotate, но это запутает логи. После discovery основной poll-loop крутится
    # по всем сессиям.
    return await _discover_upbit_max_id(sessions[0])


async def _discover_upbit_max_id(http) -> int:
    """
    Бинарный поиск max id через SINGLE-endpoint (list-endpoint защищён CF).
    Зеркало production parser_listing._discover_upbit_max_id.

    1. baseline = UPBIT_DISCOVERY_BASELINE (env override).
    2. Если baseline уже 404 — fallback на baseline (догоним инкрементом).
    3. Экспоненциальный рост: baseline+1,+2,+4,+8... пока 404 → [lo, hi].
    4. Бинарный поиск в [lo, hi] → lo = последний существующий.

    ~10-25 запросов, ~1-3с на проде с 50мс sleep между ними.
    """
    baseline = UPBIT_DISCOVERY_BASELINE

    if not await _upbit_id_exists(http, baseline):
        _log("UPBIT", f"baseline id={baseline} уже 404 — fallback на baseline")
        return baseline

    # Экспоненциальный рост.
    lo = baseline
    step = 1
    hi: int | None = None
    while step <= 1_000_000:
        probe = baseline + step
        if await _upbit_id_exists(http, probe):
            lo = probe
            step *= 2
        else:
            hi = probe
            break
        await asyncio.sleep(0.05)
    if hi is None:
        _log("UPBIT", f"discovery: не нашли границу за 1M, fallback lo={lo}")
        return lo + 1

    # Бинарный поиск.
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if await _upbit_id_exists(http, mid):
            lo = mid
        else:
            hi = mid
        await asyncio.sleep(0.05)

    return lo + 1


async def _poll_upbit(sessions: list) -> None:
    """
    sessions: list[curl_cffi.AsyncSession] — round-robin между ними на
    каждый probe-запрос. Распределяет нагрузку по нескольким Korean
    residential IP'ам и снижает шанс CF triggering на одиночном IP.
    """
    if not sessions:
        _log("UPBIT", "пустой список сессий — poll не стартует")
        return

    import itertools as _it
    session_cycle = _it.cycle(sessions)

    UPBIT_LOOKAHEAD = 3
    try:
        next_id = await _discover_upbit_max_id_via_sessions(sessions, session_cycle)
        _log("UPBIT", f"discovery: max+1 = {next_id}")
    except Exception as e:
        _log("UPBIT", f"discovery failed: {e!r} — старт с 1000000 fallback")
        next_id = 1000000

    interval = UPBIT_POLL_MS / 1000.0
    _log("UPBIT", f"poll старт ({UPBIT_POLL_MS}мс, sessions={len(sessions)}) от id={next_id}")

    while True:
        try:
            advanced = False
            for offset in range(UPBIT_LOOKAHEAD):
                probe_id = next_id + offset
                t_send = time.perf_counter()
                # Round-robin: каждый probe-запрос через следующий прокси.
                http = next(session_cycle)
                title, category = await _try_fetch_upbit_announcement(http, probe_id)
                t_recv = time.perf_counter()
                fetch_ms = (t_recv - t_send) * 1000

                if title is None:
                    if offset == 0:
                        break
                    continue

                next_id = probe_id + 1
                advanced = True

                tickers = _extract_upbit_tickers(title, category)
                if not tickers:
                    _log("UPBIT", f"skip id={probe_id} cat={category} | {title[:60]}")
                    continue

                payload = {
                    "type": "listing",
                    "source": "SEOUL-RELAY-UPBIT",
                    "coins": tickers,
                    "title": title,
                    "notice_id": probe_id,
                    "fetch_ms": round(fetch_ms, 1),
                    "t_pub_ms": int(time.time() * 1000),
                }
                _log("UPBIT", f"LISTING {tickers} id={probe_id} ({fetch_ms:.0f}мс) → broadcast({len(_clients)})")
                await _broadcast(payload)

            if not advanced:
                await asyncio.sleep(interval)
        except Exception as e:
            _log("UPBIT", f"poll err: {e!r}")
            await asyncio.sleep(2.0)


# ── Market-diff pollers (самый быстрый триггер листинга) ─────────
def _markets_to_bases(items) -> set[str]:
    """Из ответа /v1/market/all → множество base-тикеров (KRW-XXX → XXX)."""
    bases: set[str] = set()
    if not isinstance(items, list):
        return bases
    for it in items:
        if not isinstance(it, dict):
            continue
        code = it.get("market") or ""
        if "-" not in code:
            continue
        base = code.split("-", 1)[1].upper()
        # _BITHUMB_NOTICE_BANNED содержит KRW/BTC/USDT/... — не тикеры монет.
        if base and base not in _BITHUMB_NOTICE_BANNED:
            bases.add(base)
    return bases


async def _broadcast_new_market(exchange: str, coins: list, fetch_ms: float) -> None:
    payload = {
        "type":      "listing",
        "source":    f"SEOUL-RELAY-{exchange}-MARKET",
        "coins":     coins,
        "title":     f"new {exchange} market: {','.join(coins)}",
        "notice_id": 0,
        "fetch_ms":  round(fetch_ms, 1),
        "t_pub_ms":  int(time.time() * 1000),
    }
    _log(f"{exchange}-MKT", f"NEW MARKET {coins} ({fetch_ms:.0f}мс) → broadcast({len(_clients)})")
    await _broadcast(payload)


async def _poll_upbit_market(sessions: list) -> None:
    """Диффит api.upbit.com/v1/market/all через curl_cffi сессии (round-robin)."""
    if not sessions:
        return
    import itertools as _it
    cyc = _it.cycle(sessions)
    interval = MARKET_POLL_MS / 1000.0
    known: set[str] = set()
    try:
        r = await next(cyc).get(UPBIT_MARKET_URL, timeout=4)
        known = _markets_to_bases(r.json())
        _log("UPBIT-MKT", f"init: {len(known)} рынков, poll {MARKET_POLL_MS}мс")
    except Exception as e:
        _log("UPBIT-MKT", f"init failed: {e!r}")

    while True:
        await asyncio.sleep(interval)
        # Весь body в try — аддитивный поллер НЕ должен ронять relay через
        # gather при неожиданной ошибке (парсинг/сеть/broadcast).
        try:
            t_send = time.perf_counter()
            r = await next(cyc).get(UPBIT_MARKET_URL, timeout=3)
            cur = _markets_to_bases(r.json())
            if not cur:
                continue
            new = cur - known
            # known растёт монотонно (union) — защита от false-positive при
            # частичном/сбойном ответе (не считаем «исчезнувшие» монеты новыми).
            if known and new:
                await _broadcast_new_market("UPBIT", sorted(new), (time.perf_counter() - t_send) * 1000)
            known |= cur
        except Exception as e:
            _log("UPBIT-MKT", f"poll err: {e!r}")
            await asyncio.sleep(2.0)


async def _poll_bithumb_market(http: aiohttp.ClientSession) -> None:
    """Диффит api.bithumb.com/v1/market/all (aiohttp)."""
    interval = MARKET_POLL_MS / 1000.0
    known: set[str] = set()
    try:
        async with http.get(BITHUMB_MARKET_URL, timeout=aiohttp.ClientTimeout(total=4)) as r:
            r.raise_for_status()
            known = _markets_to_bases(await _aio_json(r))
        _log("BITHUMB-MKT", f"init: {len(known)} рынков, poll {MARKET_POLL_MS}мс")
    except Exception as e:
        _log("BITHUMB-MKT", f"init failed: {e!r}")

    while True:
        await asyncio.sleep(interval)
        try:
            t_send = time.perf_counter()
            async with http.get(BITHUMB_MARKET_URL, timeout=aiohttp.ClientTimeout(total=2)) as r:
                r.raise_for_status()
                cur = _markets_to_bases(await _aio_json(r))
            if not cur:
                continue
            new = cur - known
            if known and new:
                await _broadcast_new_market("BITHUMB", sorted(new), (time.perf_counter() - t_send) * 1000)
            known |= cur
        except Exception as e:
            _log("BITHUMB-MKT", f"poll err: {e!r}")
            await asyncio.sleep(2.0)


# ── Main ──────────────────────────────────────────────────────────
async def main() -> None:
    _log("BOOT", f"Seoul relay starts on :{PORT}")
    _log("BOOT", f"Bithumb poll {BITHUMB_POLL_MS}мс, Upbit poll {UPBIT_POLL_MS}мс, UPBIT_ENABLED={UPBIT_ENABLED}")
    _log("BOOT", f"RELAY_SECRET len={len(SECRET)}")

    # ── Bithumb: обычный aiohttp, CF на api.bithumb.com не блочит ────
    timeout = aiohttp.ClientTimeout(total=3, connect=2, sock_read=2)
    connector_bth = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    bithumb_session = aiohttp.ClientSession(
        timeout=timeout,
        connector=connector_bth,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        },
    )

    # ── Upbit: curl_cffi + Chrome impersonate + Korean residential proxy ──
    upbit_sessions: list = []
    if UPBIT_ENABLED:
        if not HAS_CURL_CFFI:
            _log("UPBIT", "DISABLED: curl_cffi не установлен (pip install curl_cffi)")
        else:
            proxies = _parse_upbit_proxies(UPBIT_PROXIES_RAW)
            if not proxies:
                # Без прокси — пробуем direct (на Korean DC обычно 403).
                _log("UPBIT", "WARN: UPBIT_PROXIES пуст, пробую direct (вероятно 403 от CF)")
                proxies = [""]  # один "no-proxy" session
            for i, proxy_url in enumerate(proxies, 1):
                try:
                    kwargs = {
                        "impersonate": UPBIT_IMPERSONATE,
                        "timeout": 3,
                        "headers": {
                            "Origin": "https://upbit.com",
                            "Referer": "https://upbit.com/service_center/notice",
                        },
                    }
                    if proxy_url:
                        # curl_cffi принимает proxies={"http":..., "https":...}
                        # или одну строку proxy=... (для всех схем). Используем
                        # короткую форму чтобы не повторять.
                        kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
                    sess = _CurlAsyncSession(**kwargs)
                    upbit_sessions.append(sess)
                    # Маскируем креды в логе.
                    safe = proxy_url.split("@")[-1] if "@" in proxy_url else (proxy_url or "direct")
                    _log("UPBIT", f"session #{i}/{len(proxies)} готова proxy={safe} impersonate={UPBIT_IMPERSONATE}")
                except Exception as e:  # noqa: BLE001
                    _log("UPBIT", f"session #{i} init упал: {e!r}")
            if not upbit_sessions:
                _log("UPBIT", "DISABLED: ни одна сессия не создалась")
    else:
        _log("UPBIT", "DISABLED (UPBIT_ENABLED=0) — main SG-парсер продолжит ловить Upbit")

    try:
        async with websockets.serve(
            _ws_handler,
            "0.0.0.0",
            PORT,
            ping_interval=20,
            ping_timeout=10,
            max_size=2**16,
        ):
            _log("BOOT", "WS server ready")
            tasks = [_poll_bithumb(bithumb_session)]
            if upbit_sessions:
                tasks.append(_poll_upbit(upbit_sessions))
            # Market-diff: аддитивные параллельные триггеры (часто быстрее нотиса).
            if MARKET_DIFF_ENABLED:
                tasks.append(_poll_bithumb_market(bithumb_session))
                if upbit_sessions:
                    tasks.append(_poll_upbit_market(upbit_sessions))
                _log("BOOT", f"market-diff триггеры ON (poll {MARKET_POLL_MS}мс)")
            await asyncio.gather(*tasks)
    finally:
        await bithumb_session.close()
        for sess in upbit_sessions:
            try:
                await sess.close()
            except Exception:
                pass


if __name__ == "__main__":
    # uvloop ускоряет event-loop в 2-4x — на KR edge-ноде это тиже poll-циклы
    # Upbit/Bithumb и быстрее broadcast. Опционально: если не установлен на
    # VPS — тихо падаем на stdlib asyncio.
    try:
        import uvloop  # type: ignore[import-not-found]
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        _log("BOOT", "uvloop активирован")
    except ImportError:
        _log("BOOT", "uvloop не установлен — stdlib asyncio (pip install uvloop для скорости)")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log("BOOT", "shutdown by user")
