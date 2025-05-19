import asyncio
from wallet_discount_updater import login_and_get_context
from loader import DEBUG

async def main():
    await login_and_get_context()

if __name__ == "__main__":
    if not DEBUG:
        asyncio.run(main())