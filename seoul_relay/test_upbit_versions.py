"""Перебор curl_cffi impersonate-версий против Upbit API."""
import asyncio
from curl_cffi.requests import AsyncSession

VERSIONS = [
    "chrome99", "chrome110", "chrome120", "chrome124", "chrome131",
    "chrome99_android", "safari15_5", "safari17_0", "safari17_2_ios",
    "edge99", "edge101",
]


async def probe(v):
    try:
        async with AsyncSession(impersonate=v, timeout=5) as s:
            r = await s.get(
                "https://api-manager.upbit.com/api/v1/announcements/6262",
                headers={
                    "Origin": "https://upbit.com",
                    "Referer": "https://upbit.com/service_center/notice",
                },
            )
            preview = r.text[:80].replace("\n", " ")
            return r.status_code, len(r.content), preview
    except Exception as e:
        return None, 0, f"ERR {type(e).__name__}: {e}"


async def main():
    for v in VERSIONS:
        code, size, preview = await probe(v)
        print(f"{v:20s} status={code} size={size:5d}  {preview}")


asyncio.run(main())
