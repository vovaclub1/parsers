"""Проверяем: разные ли IP у наших прокси с точки зрения Upbit
+ что отвечает Upbit для каждого прокси отдельно.
"""
import asyncio
import os
from curl_cffi.requests import AsyncSession

PROXIES = [p.strip() for p in os.getenv("UPBIT_TEST_PROXIES", "").split(",") if p.strip()]


async def check(proxy):
    label = proxy.split("@")[-1]
    async with AsyncSession(
        impersonate="chrome124",
        proxies={"http": proxy, "https": proxy},
        timeout=10,
        headers={
            "Origin": "https://upbit.com",
            "Referer": "https://upbit.com/service_center/notice",
        },
    ) as s:
        try:
            # 1. Какой IP виден внешним сервисам?
            ipinfo = await s.get("https://ipinfo.io/json", timeout=8)
            print(f"=== {label} ===")
            print(f"  ipinfo: {ipinfo.status_code}")
            print(f"  body: {ipinfo.text[:300]}")

            # 2. Что отвечает Upbit на ОДНОМ запросе?
            up = await s.get(
                "https://api-manager.upbit.com/api/v1/announcements/6262",
                timeout=8,
            )
            preview = up.text[:200].replace("\n", " ")
            print(f"  upbit: status={up.status_code} preview={preview}")
            print()
        except Exception as e:
            print(f"=== {label} ===")
            print(f"  ERR: {type(e).__name__}: {e}")
            print()


async def main():
    if not PROXIES:
        raise SystemExit("UPBIT_TEST_PROXIES не задан")
    for p in PROXIES:
        await check(p)
        # пауза между прокси чтобы не нагружать
        await asyncio.sleep(2)


asyncio.run(main())
