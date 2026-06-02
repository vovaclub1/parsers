"""Тест cookie-warmup схемы: сначала на upbit.com, потом на API."""
import asyncio
from curl_cffi.requests import AsyncSession


async def main():
    async with AsyncSession(impersonate="chrome124", timeout=5) as s:
        r1 = await s.get("https://upbit.com/service_center/notice")
        print(f"homepage: status={r1.status_code} size={len(r1.content)}")
        print(f"cookies: {dict(s.cookies)}")
        print()

        r2 = await s.get(
            "https://api-manager.upbit.com/api/v1/announcements/6262",
            headers={
                "Origin": "https://upbit.com",
                "Referer": "https://upbit.com/service_center/notice",
            },
        )
        print(f"api with cookies: status={r2.status_code} size={len(r2.content)}")
        print(f"preview: {r2.text[:300]}")


asyncio.run(main())
