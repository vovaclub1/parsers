"""Pytest bootstrap.

Тесты не должны требовать реальных API-ключей, сети или .env — поэтому
подставляем безопасные значения в окружение ДО импорта config.config,
и добавляем корень репозитория в sys.path (как это делает PYTHONPATH=/Parsers
в Docker).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Безопасные заглушки — код читает их на import-time.
os.environ.setdefault("BYBIT_API_KEY", "test-key")
os.environ.setdefault("BYBIT_SECRET_KEY", "test-secret")
os.environ.setdefault("GATEIO_API_KEY", "test-key")
os.environ.setdefault("GATEIO_SECRET_KEY", "test-secret")
os.environ.setdefault("TG_API_ID", "12345")
os.environ.setdefault("TG_API_HASH", "test-hash")
os.environ.setdefault("SESSION_DIR", "/tmp")
os.environ.setdefault("DELIST_PROXIES", "")
# Не поднимать сетевые подсистемы при импорте парсеров.
os.environ.setdefault("TREE_OF_ALPHA_WS_ENABLED", "0")
os.environ.setdefault("BYBIT_WS_TRADE_ENABLED", "0")
os.environ.setdefault("BYBIT_SYNC_WS_ENABLED", "0")
