"""Тесты защиты от массового открытия позиций (bulk-guard).

Инцидент, который эти тесты предотвращают:
    _load_upbit_tickers() падает на старте (таймаут / 403 Cloudflare / сеть),
    except-ветка ставит known = set(), значит ever_seen = set().
    Первый же успешный poll возвращает ~276 KRW-тикеров, и все они
    считаются «новыми» → process_signal открывает лонги по всему рынку.

У Bithumb и Binance такой guard был, у Upbit — нет.
"""
from __future__ import annotations

import parsers.parser_listing as pl


def test_bulk_guard_constant_exists():
    assert hasattr(pl, "MAX_NEW_TICKERS_PER_TICK")
    assert 1 <= pl.MAX_NEW_TICKERS_PER_TICK <= 25


def test_is_bulk_update_detects_mass_appearance():
    many = {f"COIN{i}" for i in range(pl.MAX_NEW_TICKERS_PER_TICK + 1)}
    assert pl._is_bulk_update(many) is True


def test_is_bulk_update_allows_normal_listing():
    """Обычный листинг — 1-3 монеты за тик, должен проходить."""
    assert pl._is_bulk_update({"PRL"}) is False
    assert pl._is_bulk_update({"PRL", "GENIUS", "OPG"}) is False


def test_is_bulk_update_empty_is_not_bulk():
    assert pl._is_bulk_update(set()) is False


def test_upbit_poller_source_has_bulk_guard():
    """REGRESSION: у Upbit-поллера не было проверки на bulk-апдейт.

    Проверяем по исходнику, что guard реально вызывается во всех трёх
    поллерах, а не только в двух.
    """
    import ast
    import inspect

    for fn_name in ("run_upbit_poller", "run_bithumb_poller",
                    "run_binance_futures_poller"):
        src = inspect.getsource(getattr(pl, fn_name))
        calls = [
            n.func.id
            for n in ast.walk(ast.parse(src))
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        ]
        assert "_is_bulk_update" in calls, (
            f"{fn_name} не вызывает _is_bulk_update — "
            f"сбой загрузки на старте приведёт к массовому открытию позиций"
        )


def test_failed_initial_load_does_not_arm_poller():
    """Если стартовая загрузка списка упала, поллер обязан пометить себя
    'непроинициализированным', а не считать пустое множество за истину."""
    assert hasattr(pl, "_initial_snapshot_ok")
