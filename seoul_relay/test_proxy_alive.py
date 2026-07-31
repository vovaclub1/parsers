"""Проверка прокси с разными схемами (http / socks5 / socks5h).
Запуск:
   docker exec seoul-relay python3 /tmp/test_proxy_alive.py
"""
import asyncio
from curl_cffi.requests import AsyncSession

# Базовые координаты — будем пробовать с разными схемами
HOSTS = [
    ("jGxDRQQi:iqTupHAT@154.219.217.67:64459"),
    ("jGxDRQQi:iqTupHAT@154.219.236.208:64769"),
    ("jGxDRQQi:iqTupHAT@135.106.76.31:63057"),
]

SCHEMES = ["socks5", "socks5h", "http"]

TARGET = "https://ipinfo.io/json"   # лёгкий, отдаёт IP и страну


async def probe(scheme: str, hostcred: str):
    proxy = f"{scheme}://{hostcred}"
    try:
        async with AsyncSession(
            proxies={"http": proxy, "https": proxy},
            timeout=8,
        ) as s:
            r = await s.get(TARGET)
            preview = r.text[:140].replace("\n", " ")
            return f"status={r.status_code}  {preview}"
    except Exception as e:
        return f"ERR {type(e).__name__}: {e}"


async def main():
    for scheme in SCHEMES:
        print(f"=== scheme={scheme} ===")
        for hostcred in HOSTS:
            label = hostcred.split("@")[-1]
            result = await probe(scheme, hostcred)
            print(f"  {label:30s} {result}")
        print()


asyncio.run(main())
