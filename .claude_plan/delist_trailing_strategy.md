# План: Нативный трейлинг-стоп для делистов

## Цель
Заменить текущую стратегию делистов (3 фиксированных TP + аварийный SL 5%) на **нативный трейлинг Bybit** — ловить «первую быструю свечу вниз» с минимальной отдачей профита.

## Текущее состояние (из исследования)

### Существующая логика (`api/delist_api.py:807-871`)
```python
def _set_tp_sl_bybit_short(ticker_name, entry_price, amount):
    sl  = entry × 1.05    # +5% аварийный стоп
    tp1 = entry × 0.92    # -8%  (20% позиции)
    tp2 = entry × 0.85    # -15% (30% позиции)
    tp3 = entry × 0.55    # -45% (50% позиции)
    
    # 4 вызова /v5/position/trading-stop (SL + 3×TP)
    # tpslMode="Partial", tpSize/slSize указаны
```

**Проблема**: фиксированные TP не адаптируются к скорости дампа. Если монета летит вниз быстро — TP1/TP2 срабатывают рано и отдают остаток движения. Если медленно — можем не дойти до TP3 и откатиться.

### Рабочий паттерн из листингов (`api/listing_api.py:265-306`)
```python
def _set_tp_sl_bybit(ticker_name, entry_price, amount):
    # Лонг: SL -8%, TP1 +4.5% (30%), trailing 3.5% (70%)
    trailing_distance = round(entry_price * 0.035, 8)
    
    _post_http2("/v5/position/trading-stop", {
        "stopLoss":     str(sl),
        "takeProfit":   str(tp1),
        "trailingStop": str(trailing_distance),  # ← нативный трейлинг
        "tpslMode":     "Partial",
        "tpSize":       tp1_size,
        "positionIdx":  1,
    })
```

**Ключ**: `trailingStop` + `activePrice` (опционально) через `/v5/position/trading-stop`. Bybit сам следит за ценой на tick-resolution, не требует софтверного потока.

---

## Новая стратегия (параметры из ответов)

| Параметр | Значение | Формула (для шорта) | Комментарий |
|----------|----------|---------------------|-------------|
| **trailingStop** | 1.0% | `entry × 0.01` | Абсолютная дистанция в USDT. Стоп идёт вниз за ценой, фиксируется при откате вверх на 1%. |
| **activePrice** | entry × 0.99 | `entry × 0.99` | Трейлинг «спит» пока цена не упадёт на 1% (мы в +1% плюсе), потом активируется. Защита от входного шума. |
| **stopLoss** (аварийный) | entry × 1.05 | `entry × 1.05` | Если цена сразу пошла ВВЕРХ против шорта до активации трейлинга — режем убыток на 5%. |
| **TP-уровни** | **убрать** | — | Чистый трейлинг, без фиксированных TP. Вся позиция закрывается по trailingStop (или аварийному SL). |

### Механика (для шорта)
1. **Вход**: `market_open_short()` → позиция открыта по `entry_price`.
2. **Сразу после входа** (`_tp_sl_executor.submit`):
   - Ставим **аварийный SL** = `entry × 1.05` (на всю позицию, `slSize=amount`).
   - Ставим **trailingStop** = `entry × 0.01` (абсолютная дистанция).
   - Ставим **activePrice** = `entry × 0.99` (трейлинг активируется когда цена упадёт ниже этого уровня).
3. **Пока цена > activePrice** (мы ещё не в +1%):
   - Трейлинг «спит», защищает только аварийный SL.
4. **Цена упала ≤ activePrice** (мы в +1% плюсе):
   - Трейлинг активируется, начинает следовать за ценой вниз.
   - Стоп = `current_price + trailing_distance` (для шорта: стоп ВЫШЕ текущей цены на 1%).
5. **Цена откатила вверх на 1%** от лучшего (минимального) уровня:
   - Bybit закрывает всю позицию по market (reduce-only).
6. **Если цена сразу пошла вверх** и пробила `entry × 1.05`:
   - Аварийный SL закрывает позицию с убытком 5%.

---

## Изменения в коде

### 1. `api/delist_api.py` — новая функция `_set_tp_sl_bybit_short()`

**Файл**: `api/delist_api.py`  
**Строки**: 807-871 (текущая реализация)

**Действие**: Полностью переписать функцию.

#### Новая реализация (псевдокод с комментариями)
```python
def _set_tp_sl_bybit_short(ticker_name: str, entry_price: float, amount: float) -> str:
    """
    Выставляет нативный trailing stop 1.0% + аварийный SL 5% для шорта на Bybit.
    
    Стратегия «первая быстрая свеча»:
      - trailingStop 1% (тугой) — максимум фиксируем с пика дампа
      - activePrice = entry × 0.99 — активация после +1% в плюс
      - аварийный SL = entry × 1.05 — защита если цена сразу пошла против
      - БЕЗ фиксированных TP — чистый трейлинг
    """
    symbol = f"{ticker_name}USDT"
    
    # Проверка qty step (как сейчас)
    try:
        step = _get_qty_step(symbol)
    except QtyStepUnavailable as e:
        print(f"[TP/SL SKIP] {e}")
        return "skip"
    
    # Параметры стратегии
    sl_price = round(entry_price * 1.05, 8)           # +5% аварийный стоп (цена ВЫШЕ входа)
    trailing_distance = round(entry_price * 0.01, 8)  # 1% абсолютная дистанция в USDT
    active_price = round(entry_price * 0.99, 8)       # Активация после -1% (цена НИЖЕ входа)
    
    sl_size = str(_round_qty(amount, step))           # Вся позиция
    
    # Один вызов /v5/position/trading-stop с полным набором параметров
    _post_http2("/v5/position/trading-stop", {
        "category":     "linear",
        "symbol":       symbol,
        "positionIdx":  2,                            # short-hedge
        
        # Аварийный стоп (если цена пошла вверх против шорта)
        "stopLoss":     str(sl_price),
        "slTriggerBy":  "LastPrice",
        "slSize":       sl_size,                      # На всю позицию
        
        # Нативный трейлинг (активируется после -1%)
        "trailingStop": str(trailing_distance),       # 1% дистанция
        "activePrice":  str(active_price),            # Активация при entry × 0.99
        
        # tpslMode НЕ нужен (нет частичных TP, только trailing на всю позицию)
    })
    
    print(
        f"[TP/SL SET SHORT] {ticker_name} | "
        f"SL={sl_price}(+5%) | Trailing=1.0% (active@{active_price})"
    )
    return "Выставил trailing stop"
```

**Ключевые отличия от текущего**:
- ❌ Убраны `tp1/tp2/tp3` и их `tpSize`.
- ❌ Убран `ThreadPoolExecutor` (был для параллельной постановки 3 TP — больше не нужен).
- ✅ Добавлены `trailingStop` + `activePrice`.
- ✅ Один вызов `/v5/position/trading-stop` вместо 4 (SL + 3×TP).
- ✅ `tpslMode` **не указываем** (он нужен только для `tpSize`/`slSize` при частичных TP/SL; здесь trailing на всю позицию — Bybit сам поймёт).

---

### 2. Gate.io fallback — оставить как есть

**Файл**: `api/gate_api.py`  
**Функция**: `gate_set_tp_sl_short()` (строки ~540-590, судя по импортам)

**Действие**: **НЕ ТРОГАТЬ**. Gate.io уже имеет софтверный trailing 6% для лонгов (`_run_trailing_stop`, строки 400-470). Для делистов (шорт) там, вероятно, фиксированные TP или простой SL — это редкий fallback (Bybit приоритетнее), не стоит усложнять.

**Обоснование**: 
- Gate.io — резервная биржа (используется только если монеты нет на Bybit).
- Переписывать `_run_trailing_stop` под шорт + 1% trailing — много работы для редкого кейса.
- Текущая Gate-логика работает, пусть остаётся.

---

### 3. Проверка совместимости с Bybit API

**Endpoint**: `POST /v5/position/trading-stop`  
**Документация**: https://bybit-exchange.github.io/docs/v5/position/trading-stop

**Параметры для нативного trailing (short)**:
```json
{
  "category": "linear",
  "symbol": "BTCUSDT",
  "positionIdx": 2,
  
  "stopLoss": "105000.0",       // Аварийный SL (выше entry для short)
  "slTriggerBy": "LastPrice",
  "slSize": "0.01",             // Вся позиция
  
  "trailingStop": "1000.0",     // Абсолютная дистанция в USDT (1% от entry)
  "activePrice": "99000.0"      // Активация (ниже entry для short)
}
```

**Проверка по документации**:
- ✅ `trailingStop` поддерживается для linear (USDT perp).
- ✅ `activePrice` опционален — если не указан, trailing активен с первого тика.
- ✅ `stopLoss` + `trailingStop` могут быть в одном запросе (аварийный SL + trailing).
- ✅ Для short: `trailingStop` — абсолютное расстояние ВВЕРХ от текущей цены (стоп выше цены).
- ⚠️ `tpslMode` **не нужен** если нет `tpSize` (trailing на всю позицию = Full mode по умолчанию).

**Риск**: Если Bybit требует `tpslMode="Full"` явно при использовании `trailingStop` без `tpSize` — добавим в код. Проверим в тестах.

---

### 4. Удаление старых TP-констант (опционально)

**Файл**: `api/delist_api.py`  
**Строки**: 811-814 (текущие)

```python
sl  = round(entry_price * 1.05, 8)
tp1 = round(entry_price * 0.92, 8)   # ← больше не нужны
tp2 = round(entry_price * 0.85, 8)   # ← больше не нужны
tp3 = round(entry_price * 0.55, 8)   # ← больше не нужны
```

**Действие**: Удалить `tp1/tp2/tp3` и связанные `tp1_size/tp2_size/tp3_size` (строки 824-826).

---

### 5. Env-переменные (опционально, для тюнинга без передеплоя)

**Файл**: `config/config.py`  
**Новые переменные** (добавить в конец):

```python
# Delist trailing stop strategy (native Bybit trailing)
DELIST_TRAILING_PCT = float(os.getenv("DELIST_TRAILING_PCT", "0.01"))      # 1.0% default
DELIST_ACTIVE_PCT   = float(os.getenv("DELIST_ACTIVE_PCT", "0.01"))        # Активация после +1%
DELIST_SL_PCT       = float(os.getenv("DELIST_SL_PCT", "0.05"))            # Аварийный SL +5%
```

**Использование в `delist_api.py`**:
```python
from config.config import DELIST_TRAILING_PCT, DELIST_ACTIVE_PCT, DELIST_SL_PCT

def _set_tp_sl_bybit_short(...):
    sl_price = round(entry_price * (1 + DELIST_SL_PCT), 8)
    trailing_distance = round(entry_price * DELIST_TRAILING_PCT, 8)
    active_price = round(entry_price * (1 - DELIST_ACTIVE_PCT), 8)
    ...
```

**Обоснование**: Если 1% trailing окажется слишком тугим (частые ложные срабатывания) — можно поднять до 1.5% через `.env` без передеплоя.

---

## Тестирование

### 1. Unit-тест (локально, без реальных ордеров)
**Цель**: Проверить что `_set_tp_sl_bybit_short()` формирует правильный payload.

```python
# test_delist_trailing.py
def test_trailing_stop_payload():
    ticker = "TEST"
    entry = 100.0
    amount = 1.0
    
    # Mock _post_http2 и перехватываем params
    with patch('api.delist_api._post_http2') as mock_post:
        _set_tp_sl_bybit_short(ticker, entry, amount)
        
        call_args = mock_post.call_args[0][1]  # params dict
        
        assert call_args["stopLoss"] == "105.0"          # +5%
        assert call_args["trailingStop"] == "1.0"        # 1% от 100
        assert call_args["activePrice"] == "99.0"        # -1%
        assert call_args["slSize"] == "1.0"
        assert "takeProfit" not in call_args             # Нет TP
        assert "tpSize" not in call_args
```

### 2. Staging-тест (на Bybit testnet или малой позиции)
**Цель**: Проверить что Bybit принимает запрос и trailing работает.

**План**:
1. Открыть тестовый шорт на 10 USDT (монета с низкой волатильностью, например BTC).
2. Вызвать `_set_tp_sl_bybit_short()`.
3. Проверить через Bybit UI / API `GET /v5/position/list`:
   - `stopLoss` = entry × 1.05 ✓
   - `trailingStop` = entry × 0.01 ✓
   - `activePrice` = entry × 0.99 ✓
4. Дождаться движения цены вниз на 1% → trailing должен активироваться.
5. Симулировать откат вверх на 1% → позиция должна закрыться.

**Риски**:
- Если Bybit вернёт ошибку «tpslMode required» → добавить `"tpslMode": "Full"` в payload.
- Если `activePrice` не работает для short → убрать его (trailing будет активен сразу).

### 3. Production-мониторинг (первые 24ч)
**Цель**: Убедиться что стратегия работает на реальных делистах.

**Метрики**:
- Сколько позиций закрылось по trailing (vs аварийному SL)?
- Средний профит на закрытие (должен быть выше старых TP1/TP2).
- Частота ложных срабатываний (trailing сработал на микро-откате до большого дампа).

**Логирование** (добавить в `_set_tp_sl_bybit_short`):
```python
print(
    f"[TP/SL SET SHORT] {ticker_name} | "
    f"entry={entry_price} | SL={sl_price}(+5%) | "
    f"Trailing=1.0% (active@{active_price})"
)
```

**Алерт** (если нужно):
- Если >50% позиций закрываются по аварийному SL (а не по trailing) → trailing слишком тугой или activePrice слишком высокий.

---

## Откат (rollback plan)

Если новая стратегия работает хуже старой (меньше профита / больше убытков):

1. **Быстрый откат** (без передеплоя):
   ```bash
   # В .env на сервере
   DELIST_TRAILING_PCT=0.0  # Отключает trailing (fallback на старую логику)
   docker-compose restart listing
   ```
   
2. **Полный откат** (git revert):
   ```bash
   git revert <commit_hash>
   git push origin test/batch-7-keywords
   # На сервере: git pull && docker-compose up -d --build listing
   ```

3. **Гибридный вариант** (если trailing хорош, но нужны TP):
   - Вернуть TP3 (-45%, 50% позиции) как «страховку» на случай глубокого дампа.
   - Trailing на оставшиеся 50%.
   - Потребует `tpslMode="Partial"` + `tpSize`.

---

## Чеклист перед коммитом

- [ ] `_set_tp_sl_bybit_short()` переписана (trailing + activePrice + аварийный SL).
- [ ] Удалены старые `tp1/tp2/tp3` и `ThreadPoolExecutor` для TP.
- [ ] Добавлены env-переменные `DELIST_TRAILING_PCT` / `DELIST_ACTIVE_PCT` / `DELIST_SL_PCT` в `config/config.py`.
- [ ] Логирование обновлено (новый формат с trailing-параметрами).
- [ ] `gate_set_tp_sl_short()` НЕ тронута (Gate.io fallback остаётся как есть).
- [ ] Unit-тест написан и проходит.
- [ ] Staging-тест на testnet / малой позиции выполнен.
- [ ] Коммит-сообщение описывает изменение стратегии.

---

## Коммит-сообщение (draft)

```
feat(delist): нативный trailing stop 1% вместо фиксированных TP

Заменяем стратегию делистов с 3 фиксированных TP (-8%/-15%/-45%)
на нативный Bybit trailingStop 1.0% — ловим «первую быструю свечу»
с минимальной отдачей профита.

Параметры:
- trailingStop: 1.0% (тугой, максимум фиксируем с пика)
- activePrice: entry × 0.99 (активация после +1% в плюс)
- аварийный SL: entry × 1.05 (+5% защита если цена пошла против)
- БЕЗ фиксированных TP — чистый трейлинг на всю позицию

Изменения:
- api/delist_api.py: _set_tp_sl_bybit_short() переписана
- config/config.py: добавлены DELIST_TRAILING_PCT/ACTIVE_PCT/SL_PCT
- Удалены tp1/tp2/tp3 и ThreadPoolExecutor для TP
- Gate.io fallback не тронут (редкий кейс)

Паттерн взят из listing_api.py (там trailing 3.5% на лонгах работает).
Env-tunable: можно менять % через .env без передеплоя.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
```

---

## Итого

**Что делаем**:
1. Переписываем `_set_tp_sl_bybit_short()` — один вызов `/v5/position/trading-stop` с `trailingStop` + `activePrice` + аварийным `stopLoss`.
2. Удаляем старые TP1/TP2/TP3 и их параллельную постановку.
3. Добавляем env-переменные для тюнинга без передеплоя.
4. Тестируем на testnet / малой позиции.
5. Мониторим первые 24ч в проде.

**Что НЕ делаем**:
- Не трогаем Gate.io (fallback остаётся как есть).
- Не пишем софтверный трейлинг (нативный Bybit проще и надёжнее).
- Не меняем логику входа (`market_open_short` остаётся).

**Риски**:
- Trailing 1% может быть слишком тугим → env-tunable, можно поднять до 1.5-2%.
- `activePrice` может не работать для short → уберём, trailing будет активен сразу.
- Bybit может требовать `tpslMode="Full"` явно → добавим если API вернёт ошибку.

**Выигрыш**:
- Tick-resolution (Bybit следит за ценой на каждом тике, не раз в 2с как price_cache).
- Адаптивность (trailing подстраивается под скорость дампа, фиксированные TP — нет).
- Простота (один вызов API вместо 4, нет софтверного потока-наблюдателя).
- Надёжность (переживает рестарт контейнера, не зависит от сети бота).
