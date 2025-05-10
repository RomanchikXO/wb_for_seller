import asyncio
from playwright_utils import login_and_get_context, get_and_store_cookies

async def main():
    # page = await login_and_get_context()  # ручной вход 1 раз
    while True:
        # await get_and_store_cookies(page)
        await asyncio.sleep(300)  # каждые 5 минут

if __name__ == "__main__":
    asyncio.run(main())
