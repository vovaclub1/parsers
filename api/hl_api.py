from __future__ import annotations

# ── hl_api.py ─────────────────────────────────────────────────────
# Hyperliquid фьючерсы (перп-контракты).
# Используется в арбитраже как третья биржа наряду с Bybit и Gate.io.
#
# Как работает аутентификация на HL:
#   - НЕТ API-ключей (как на CEX). Только приватный ключ кошелька.
#   - Торговые L1-действия подписываются через EIP-712 (Msgpack).
#   - SDK (hyperliquid-python-sdk) делает всё это внутри сам.
#
# Требования:
#   pip install hyperliquid-python-sdk eth-account
#
# Переменные в .env:
#   HL_PRIVATE_KEY = 0x...  (приватный ключ кошелька, с 0x)
#   HL_ADDRESS     = 0x...  (публичный адрес кошелька)
#
# Важные нюансы SDK:
#   1. Exchange(wallet, base_url, account_address=address)
#      account_address нужен ТОЛЬКО если торгуешь через API-кошелёк
#      (agent key), а не через основной. Если ключ = основной кошелёк,
#      account_address не нужен.
#   2. exchange.market_open(coin, is_buy, sz, slippage=0.05)
#      sz — в единицах монеты (не в USDT!), с учётом szDecimals.
#   3. exchange.market_close(coin) — закрывает всю позицию.
#   4. exchange.update_leverage(leverage, coin, is_cross=True)
#      Надо вызвать ДО открытия позиции.
#   5. Цены кэшируем сами через /info allMids — SDK Info тоже умеет,
#      но мы хотим контролировать скорость обновления.
#   6. szDecimals — количество знаков после запятой для размера ордера.
#      Берётся из meta().universe[i].szDecimals.
#      Нарушение → ордер отклонится с ошибкой про размер.
# ─────────────────────────────────────────────────────────────────

import os
import time
import threading
import requests

from config.config import HYPERLIQUID_KEY

# ── ANSI цвета ────────────────────────────────────────────────────
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"
RESET   = "\033[0m"

def _tag(label: str, color: str) -> str:
    return f"{color}{BOLD}[{label}]{RESET}"


# ── Константы ─────────────────────────────────────────────────────
_HL_BASE     = "https://api.hyperliquid.xyz"
_HL_INFO_URL = f"{_HL_BASE}/info"
_LEVERAGE    = 10
_SLIPPAGE    = 0.03   # 3% slippage для маркет-ордеров через SDK

# ── Приватный ключ и адрес ────────────────────────────────────────
# HL_PRIVATE_KEY должен быть с "0x" в начале
_HL_PRIVATE_KEY: str = HYPERLIQUID_KEY or os.getenv("HL_PRIVATE_KEY", "")
_HL_ADDRESS:     str = os.getenv("HL_ADDRESS", "")

# ── Кэш цен ───────────────────────────────────────────────────────
hl_price_cache: dict[str, float] = {}
hl_known_coins: set[str]         = set()
_cache_lock = threading.Lock()

# ── Кэш szDecimals ────────────────────────────────────────────────
# szDecimals[coin] = количество знаков после запятой для размера ордера.
# Например BTC=5 (0.00001), ETH=4 (0.0001), DOGE=0 (1).
_sz_decimals: dict[str, int] = {}
_sz_lock = threading.Lock()

# ── HTTP-сессия для публичных запросов ────────────────────────────
_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})

# ── SDK Exchange (создаётся лениво при первом торговом вызове) ────
_exchange      = None
_exchange_lock = threading.Lock()


# ── Публичные REST запросы ────────────────────────────────────────

def _info(payload: dict) -> dict | list:
    """
    POST к /info — публичный эндпоинт, без подписи.
    Возвращает dict или list в зависимости от type.
    """
    resp = _session.post(_HL_INFO_URL, json=payload, timeout=5)
    resp.raise_for_status()
    return resp.json()


# ── Инициализация SDK Exchange ────────────────────────────────────

def _get_exchange():
    """
    Лениво создаёт и кэширует SDK Exchange.
    Exchange — синглтон: создаём один раз, переиспользуем.
    Важно: skip_ws=True в Info чтобы не открывать лишние WS-соединения.
    """
    global _exchange
    if _exchange is not None:
        return _exchange

    with _exchange_lock:
        if _exchange is not None:
            return _exchange

        if not _HL_PRIVATE_KEY:
            raise RuntimeError(
                "[HL] ОШИБКА: задай HL_PRIVATE_KEY (с 0x) в .env\n"
                "      Пример: HL_PRIVATE_KEY=0xabcdef..."
            )

        try:
            import eth_account
            from hyperliquid.exchange import Exchange
            from hyperliquid.utils import constants

            wallet = eth_account.Account.from_key(_HL_PRIVATE_KEY)

            # Если _HL_ADDRESS задан и отличается от wallet.address —
            # значит secret_key это API-кошелёк (agent), а account_address
            # это основной кошелёк. Передаём оба.
            # Если не задан или совпадает — торгуем напрямую основным кошельком.
            if _HL_ADDRESS and _HL_ADDRESS.lower() != wallet.address.lower():
                print(
                    f"{_tag('HL', CYAN)} Режим API-wallet: "
                    f"agent={wallet.address[:8]}... account={_HL_ADDRESS[:8]}..."
                )
                _exchange = Exchange(
                    wallet,
                    constants.MAINNET_API_URL,
                    account_address=_HL_ADDRESS,
                )
            else:
                print(
                    f"{_tag('HL', CYAN)} Режим основного кошелька: "
                    f"{wallet.address[:8]}..."
                )
                _exchange = Exchange(wallet, constants.MAINNET_API_URL)

            print(f"{_tag('HL', GREEN)} Exchange SDK инициализирован ✓")

        except ImportError as e:
            raise RuntimeError(
                f"[HL] Не установлен SDK: {e}\n"
                "      pip install hyperliquid-python-sdk eth-account"
            ) from e

    return _exchange


# ── Кэш szDecimals ────────────────────────────────────────────────

def _load_sz_decimals() -> None:
    """
    Загружает szDecimals для всех монет через meta().
    Нужно вызвать один раз при старте (или при обновлении кэша).
    Без этого round_sz будет использовать fallback (2 знака) и
    ордера могут отклоняться с ошибкой размера.
    """
    try:
        data     = _info({"type": "meta"})
        universe = data.get("universe", [])
        with _sz_lock:
            for asset in universe:
                name = asset.get("name", "")
                dec  = asset.get("szDecimals", 2)
                if name:
                    _sz_decimals[name] = dec
        print(f"{_tag('HL META', CYAN)} szDecimals загружены для {len(_sz_decimals)} монет")
    except Exception as e:
        print(f"{_tag('HL META ERR', RED)} не удалось загрузить szDecimals: {e}")


def _get_sz_decimals(coin: str) -> int:
    """
    Возвращает szDecimals для монеты.
    Если не в кэше — запрашивает meta ещё раз.
    Fallback: 4 (подходит для большинства alt-монет).
    """
    with _sz_lock:
        if coin in _sz_decimals:
            return _sz_decimals[coin]

    # Монеты не было в кэше → обновляем
    _load_sz_decimals()

    with _sz_lock:
        return _sz_decimals.get(coin, 4)


def _round_sz(sz: float, coin: str) -> float:
    """
    Округляет размер ордера до szDecimals знаков после запятой.
    Hyperliquid отклонит ордер если размер имеет лишние знаки.
    """
    decimals = _get_sz_decimals(coin)
    return round(sz, decimals)


# ── Price Updater ─────────────────────────────────────────────────

def hl_price_updater() -> None:
    """
    Бесконечный цикл — каждые 3 секунды обновляет кэш цен с Hyperliquid.
    Использует /info allMids — самый лёгкий публичный эндпоинт.
    Запускать в daemon-потоке.
    """
    print(f"{_tag('HL', CYAN)} price_updater запущен")
    while True:
        try:
            data = _info({"type": "allMids"})   # {coin: "price_str", ...}

            new_cache: dict[str, float] = {}
            new_coins: set[str]         = set()

            for coin, price_str in data.items():
                try:
                    price = float(price_str)
                    if price > 0:
                        new_cache[coin] = price
                        new_coins.add(coin)
                except (ValueError, TypeError):
                    pass

            with _cache_lock:
                hl_price_cache.clear()
                hl_price_cache.update(new_cache)
                hl_known_coins.clear()
                hl_known_coins.update(new_coins)

        except Exception as e:
            print(f"{_tag('HL ERR', RED)} price_updater: {e}")

        time.sleep(3)


def hl_get_price(coin: str) -> float:
    """Возвращает mid-цену из кэша Hyperliquid. 0 если монеты нет."""
    with _cache_lock:
        return hl_price_cache.get(coin, 0)


# ── Фандинг ───────────────────────────────────────────────────────

def hl_get_funding(coin: str) -> tuple[float, float]:
    """
    Возвращает (funding_rate, next_funding_time_unix) с Hyperliquid.
    HL начисляет фандинг каждый час (в отличие от Bybit/Gate каждые 8ч).
    funding_rate — ставка за один час.
    """
    try:
        data     = _info({"type": "metaAndAssetCtxs"})
        universe = data[0].get("universe", [])
        ctxs     = data[1]

        for i, asset in enumerate(universe):
            if asset.get("name") == coin:
                ctx       = ctxs[i]
                rate      = float(ctx.get("funding", 0))
                now       = time.time()
                # HL начисляет фандинг в начале каждого часа
                next_time = now - (now % 3600) + 3600
                return rate, next_time

        return 0.0, 0.0
    except Exception as e:
        print(f"{_tag('HL FUNDING ERR', RED)} {coin}: {e}")
        return 0.0, 0.0


# ── Прогрев соединения ────────────────────────────────────────────

def warmup_hl_connection() -> None:
    """
    Прогревает HTTP соединение к Hyperliquid и загружает szDecimals.
    Вызывать при старте перед первой сделкой.
    """
    try:
        _info({"type": "allMids"})
        _load_sz_decimals()
        print(f"{_tag('HL WARMUP', GREEN)} соединение прогрето ✓")
    except Exception as e:
        print(f"{_tag('HL WARMUP ERR', RED)} {e}")


# ── Установка плеча ───────────────────────────────────────────────

def _hl_set_leverage(coin: str, leverage: int = _LEVERAGE, is_cross: bool = True) -> None:
    """
    Устанавливает плечо для монеты перед открытием позиции.
    is_cross=True → кросс-маржа (рекомендуется для арбитража).
    is_cross=False → изолированная маржа.
    """
    try:
        ex = _get_exchange()
        ex.update_leverage(leverage, coin, is_cross=is_cross)
        mode = "cross" if is_cross else "isolated"
        print(f"{_tag('HL LEV', CYAN)} {coin} → {leverage}x {mode}")
    except Exception as e:
        print(f"{_tag('HL LEV ERR', RED)} {coin} {leverage}x: {e}")


# ── Расчёт размера позиции ────────────────────────────────────────

def _calc_sz(coin: str, usdt_amount: float, leverage: int = _LEVERAGE) -> float:
    """
    Считает размер позиции в единицах монеты.
    sz = (usdt_amount * leverage) / price
    Округляет до szDecimals.
    :return: float — размер в единицах монеты, 0 если нет цены.
    """
    price = hl_get_price(coin)
    if not price:
        print(f"{_tag('HL NO PRICE', RED)} {coin}: нет цены в кэше")
        return 0.0
    raw_sz = (usdt_amount * leverage) / price
    return _round_sz(raw_sz, coin)


# ── Маркет-ордера через SDK ───────────────────────────────────────

def _hl_market_open(coin: str, is_buy: bool, sz: float) -> tuple[float, float]:
    """
    Открывает рыночную позицию через SDK exchange.market_open().
    SDK сам подписывает транзакцию и добавляет slippage к limit_px.

    Ответ SDK:
      {"status": "ok", "response": {"type": "order", "data": {"statuses": [
        {"filled": {"oid": ..., "totalSz": "0.05", "avgPx": "65000.0"}}
      ]}}}

    :return: (fill_sz, fill_price) или (0, 0) при ошибке.
    """
    try:
        ex     = _get_exchange()
        result = ex.market_open(coin, is_buy, sz, slippage=_SLIPPAGE)

        if not isinstance(result, dict):
            print(f"{_tag('HL ORDER ERR', RED)} {coin} | SDK вернул: {result!r}")
            return 0, 0

        if result.get("status") != "ok":
            print(f"{_tag('HL ORDER ERR', RED)} {coin} | status={result.get('status')} | {result}")
            return 0, 0

        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        if not statuses:
            print(f"{_tag('HL ORDER ERR', RED)} {coin} | пустой statuses")
            return 0, 0

        status = statuses[0]

        # Успешное исполнение → ключ "filled"
        if "filled" in status:
            filled     = status["filled"]
            fill_sz    = float(filled.get("totalSz", sz))
            fill_price = float(filled.get("avgPx", hl_get_price(coin) or 0))
            return fill_sz, fill_price

        # Ошибка → ключ "error"
        if "error" in status:
            print(f"{_tag('HL ORDER ERR', RED)} {coin} | {status['error']}")
            return 0, 0

        # Ордер выставлен но не заполнен (resting) — IOC не должен так работать
        if "resting" in status:
            print(f"{_tag('HL ORDER WARN', YELLOW)} {coin} | ордер resting (не исполнен?): {status}")
            return 0, 0

        print(f"{_tag('HL ORDER UNKNOWN', YELLOW)} {coin} | неизвестный статус: {status}")
        return 0, 0

    except Exception as e:
        print(f"{_tag('HL SDK ERR', RED)} {coin} market_open: {e}")
        return 0, 0


def _hl_market_close(coin: str, sz: float | None = None) -> bool:
    """
    Закрывает позицию через SDK exchange.market_close().
    sz=None → закрывает всю позицию.
    :return: True если успешно.
    """
    try:
        ex     = _get_exchange()
        result = ex.market_close(coin, sz=sz, slippage=_SLIPPAGE)

        if not isinstance(result, dict) or result.get("status") != "ok":
            print(f"{_tag('HL CLOSE ERR', RED)} {coin} | {result}")
            return False

        statuses = result.get("response", {}).get("data", {}).get("statuses", [])
        if statuses and "error" in statuses[0]:
            print(f"{_tag('HL CLOSE ERR', RED)} {coin} | {statuses[0]['error']}")
            return False

        return True

    except Exception as e:
        print(f"{_tag('HL SDK ERR', RED)} {coin} market_close: {e}")
        return False


# ── Публичные торговые функции ────────────────────────────────────

def hl_open_long(coin: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает лонг на Hyperliquid.
    Автоматически:
      1. Считает sz в единицах монеты через szDecimals
      2. Устанавливает плечо _LEVERAGE x cross
      3. Открывает маркет-ордер через SDK

    :param coin: str - тикер монеты (например "ETH", "BTC")
    :param usdt_amount: float - маржа в USDT (без плеча)
    :return: (fill_sz, fill_price) или (0, 0) при ошибке
    """
    sz = _calc_sz(coin, usdt_amount, _LEVERAGE)
    if sz <= 0:
        return 0, 0

    _hl_set_leverage(coin, _LEVERAGE, is_cross=True)

    fill_sz, fill_price = _hl_market_open(coin, is_buy=True, sz=sz)
    if fill_sz:
        print(
            f"{_tag('HL LONG', GREEN)} {BOLD}{coin}{RESET}{GREEN} | "
            f"sz={fill_sz} | price≈{fill_price}"
        )
    return fill_sz, fill_price


def hl_open_short(coin: str, sz: float) -> tuple[float, float]:
    """
    Открывает шорт на Hyperliquid на заданное количество монет.
    Используется в арбитраже где размер уже посчитан из лонга на другой бирже.

    :param coin: str - тикер монеты
    :param sz: float - размер в единицах монеты (уже с учётом плеча)
    :return: (fill_sz, fill_price) или (0, 0) при ошибке
    """
    sz_rounded = _round_sz(sz, coin)
    if sz_rounded <= 0:
        print(f"{_tag('HL SHORT ERR', RED)} {coin}: sz={sz} → после округления 0")
        return 0, 0

    _hl_set_leverage(coin, _LEVERAGE, is_cross=True)

    fill_sz, fill_price = _hl_market_open(coin, is_buy=False, sz=sz_rounded)
    if fill_sz:
        print(
            f"{_tag('HL SHORT', RED)} {BOLD}{coin}{RESET}{RED} | "
            f"sz={fill_sz} | price≈{fill_price}"
        )
    return fill_sz, fill_price


def hl_open_short_by_usdt(coin: str, usdt_amount: float) -> tuple[float, float]:
    """
    Открывает шорт на Hyperliquid на заданную сумму в USDT.
    Удобно если не хочется считать sz снаружи.

    :param coin: str - тикер монеты
    :param usdt_amount: float - маржа в USDT
    :return: (fill_sz, fill_price) или (0, 0) при ошибке
    """
    sz = _calc_sz(coin, usdt_amount, _LEVERAGE)
    if sz <= 0:
        return 0, 0
    return hl_open_short(coin, sz)


def hl_close_position(coin: str, sz: float | None = None, side: str | None = None) -> None:
    """
    Закрывает позицию на Hyperliquid.
    sz=None → закрывает всю позицию (market_close).
    side игнорируется — HL сам определяет направление по текущей позиции.

    :param coin: str - тикер монеты
    :param sz: float | None - размер для закрытия, None = вся позиция
    :param side: str | None - "long" | "short" (игнорируется, для совместимости)
    """
    ok = _hl_market_close(coin, sz=sz)
    direction = side or "?"
    status    = "✓" if ok else "✗"
    print(
        f"{_tag('HL CLOSE', YELLOW)} {coin} | "
        f"side={direction} sz={sz or 'all'} → {status}"
    )