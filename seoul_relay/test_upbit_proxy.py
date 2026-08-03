"""Smoke test одного прокси против Upbit. Запуск:
   docker exec -e PROXY='socks5://user:pass@host:port' seoul-relay python3 /tmp/test_upbit_proxy.py
"""
import asyncio
import os
from curl_cffi.requests import AsyncSession

PROXY = os.environ.get("PROXY", "")
print(f"PROXY = {PROXY.split('@')[-1] if '@' in PROXY else PROXY or 'DIRECT'}")


async def main():
    kwargs = {
        "impersonate": "chrome124",
        "timeout": 8,
        "headers": {
            "Origin": "https://upbit.com",
            "Referer": "https://upbit.com/service_center/notice",
        },
    }
    if PROXY:
        kwargs["proxies"] = {"http": PROXY, "https": PROXY}

    async with AsyncSession(**kwargs) as s:
        for ann_id in [6258, 6260, 6262, 6263, 6265]:
            try:
                r = await s.get(
                    f"https://api-manager.upbit.com/api/v1/announcements/{ann_id}"
                )
                preview = r.text[:120].replace("\n", " ")
                print(f"id={ann_id} status={r.status_code} size={len(r.content):5d}  {preview}")
            except Exception as e:
                print(f"id={ann_id} ERR {type(e).__name__}: {e}")


asyncio.run(main())
