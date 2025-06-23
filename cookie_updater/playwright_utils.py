import asyncio
import time

from BOT.loader_bot import bot
from playwright.async_api import async_playwright
from database.DataBase import async_connect_to_database
import logging
from context_logger import ContextLogger
from BOT.states import set_status, get_status
logger = ContextLogger(logging.getLogger("cookie_updater"))


proxy_host='45.13.192.129'
proxy_user='31806a1a'
proxy_pass='6846a6171a'
proxy_port='30018'
proxy_url = f"http://{proxy_host}:{proxy_port}"


def ask_user_for_input(user_id):
    msg = bot.send_message(user_id, "Введите код из SMS:")
    set_status("get_sms_code", user_id)



async def login_and_get_context():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        proxy={
            "server": proxy_url,
            "username": proxy_user,
            "password": proxy_pass,
        },
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-software-rasterizer",
            "--start-maximized",
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
            "--disable-gpu",
        ]
    )
    context = await browser.new_context(
            timezone_id="Europe/Moscow",  # Устанавливаем часовой пояс на Москву
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",  # Стандартный пользовательский агент
            locale="ru-RU",  # Устанавливаем локаль
            geolocation={"latitude": 55.7558, "longitude": 37.6173},  # Геолокация Москвы
            permissions=["geolocation"],  # Разрешаем использование геолокации
        )
    page = await context.new_page()

    await page.goto("https://seller-auth.wildberries.ru/ru/?redirect_url=https%3A%2F%2Fseller.wildberries.ru%2F&fromSellerLanding")
    await page.wait_for_selector('input[data-testid="phone-input"]')

    # Введём номер (формат: 9999999999)
    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в login_and_get_context")
        return
    try:
        request = ("SELECT number, tg_id "
                   "FROM myapp_wblk "
                   "WHERE groups_id = 1 "
                   "LIMIT 1")
        all_fields = await conn.fetch(request)
        result = [{ "number": row["number"], "tg_id": row["tg_id"] } for row in all_fields]
        result = result[0]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_wblk. Запрос {request}. Error: {e}")
    finally:
        await conn.close()

    await page.fill('input[data-testid="phone-input"]', str(result["number"]))

    # Клик по кнопке со стрелкой
    await page.locator('img[src*="arrow-circle-right"]').click()

    # Ждём появления поля для ввода SMS-кода
    await page.wait_for_selector('input[data-testid="sms-code-input"]')

    # спрашиваем в боте код
    ask_user_for_input(result["tg_id"])

    sms_code = None
    while not sms_code:
        status = get_status(result["tg_id"])
        if status and status.startswith("code_"):
            sms_code = str(status.replace("code_", ""))
            break
        time.sleep(10)
    try:
        await page.fill('input[data-testid="sms-code-input"]', str(sms_code))
    except Exception as e:
        logger.error(f"Ошибка при вставке смс. Код из смс: {sms_code}. Ошибка: {e}")
        raise


    # Убеждаемся, что авторизация прошла и редирект на seller.wildberries.ru
    await page.wait_for_url("https://seller.wildberries.ru/**", timeout=60000)
    return page


async def get_and_store_cookies(page):

    try:
        await page.wait_for_load_state("networkidle")
    except:
        time.sleep(10)
    try:
        close_button = page.locator('button[class*="s__2G0W7HmatG"]')
        await close_button.wait_for(state="visible", timeout=10000)
        await close_button.click(timeout=5000)
    except Exception as e:
        logger.error(f"❌ Кнопка не нажалась: {e}")

    await page.hover("button:has-text('Pear Home')")
    await page.wait_for_selector("li.suppliers-list_SuppliersList__item__GPkdU")

    cookies_need = [
        "wbx-validation-key",
        "_wbauid",
        "x-supplier-id-external",
    ]

    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в login_and_get_context")
        return
    try:
        request = ("SELECT id, inn "
                   "FROM myapp_wblk "
                   "WHERE groups_id = 1")
        all_fields = await conn.fetch(request)
        inns = [{ "id": row["id"], "inn": row["inn"] } for row in all_fields]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_wblk. Запрос {request}. Error: {e}")
    finally:
        await conn.close()
    for inn in inns: # тут inns это массив с инн с БД
        authorizev3 = None
        async def log_request(request):
            nonlocal authorizev3
            if "/banner-homepage/suppliers-home-page/api/v2/banners" in request.url and "authorizev3" in request.headers:
                authorizev3 = request.headers["authorizev3"]

        page.on("request", log_request)

        target_text = f"ИНН {inn['inn']}"
        supplier_radio_label = page.locator(
            f"li.suppliers-list_SuppliersList__item__GPkdU:has-text('{target_text}') label[data-testid='supplier-checkbox-checkbox']"
        )
        await supplier_radio_label.wait_for(state="visible", timeout=5000)
        await supplier_radio_label.click()

        # try:
        #     async with page.expect_navigation(timeout=30000):
        #         await supplier_radio_label.click() # Таймаут 30 секунд, можно увеличить, если нужно
        # except Exception as e:
        #     logger.error(f"Это логгер. Не дождлались определённого изменения на странице. {e}")
        time.sleep(5)
        cookies = await page.context.cookies()
        cookies_str = ";".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies if cookie.get("name", "") in cookies_need)

        conn = await async_connect_to_database()
        if not conn:
            logger.warning("Ошибка подключения к БД в get_and_store_cookies")
            return
        try:
            query = """
                    UPDATE myapp_wblk 
                    SET
                        cookie = $1,
                        authorizev3 = $2
                    WHERE id = $3
                """
            await conn.execute(query, cookies_str, authorizev3, inn["id"])
        except Exception as e:
            logger.error(f"Ошибка обновления кукков в лк. Error: {e}")
        finally:
            await conn.close()
    await asyncio.sleep(300)
    await page.reload()
    await get_and_store_cookies(page)

