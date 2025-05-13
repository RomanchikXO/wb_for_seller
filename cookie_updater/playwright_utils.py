import asyncio
import time

from BOT.loader_bot import bot
from playwright.async_api import async_playwright
from database.DataBase import async_connect_to_database
from database.funcs_db import add_set_data_from_db
import logging
from context_logger import ContextLogger
from BOT.states import set_status, get_status
logger = ContextLogger(logging.getLogger("cookie_updater"))


def ask_user_for_input(user_id):
    msg = bot.send_message(user_id, "Введите код из SMS:")
    set_status("get_sms_code", user_id)



async def login_and_get_context():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
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
        print(f"Ошибка при вставке смс. Код из смс: {sms_code}. Ошибка: {e}")
        logger.error(f"Ошибка при вставке смс. Код из смс: {sms_code}. Ошибка: {e}")
        raise


    # Убеждаемся, что авторизация прошла и редирект на seller.wildberries.ru
    await page.wait_for_url("https://seller.wildberries.ru/**", timeout=60000)

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

    try:
        await page.hover("button:has-text('ИП Элларян А. А.')")
    except Exception as e:
        await page.hover("button:has-text('Pear Home')")
    await page.wait_for_selector("li.suppliers-list_SuppliersList__item__GPkdU")

    return page


async def get_and_store_cookies(page):

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


        target_text = f"ИНН {inn['inn']}"
        supplier_radio_label = page.locator(
            f"li.suppliers-list_SuppliersList__item__GPkdU:has-text('{target_text}') label[data-testid='supplier-checkbox-checkbox']"
        )
        await supplier_radio_label.wait_for(state="visible", timeout=5000)
        await supplier_radio_label.click()

        async with page.expect_navigation(timeout=30000):
            await supplier_radio_label.click() # Таймаут 30 секунд, можно увеличить, если нужно

        cookies = await page.context.cookies()
        cookies_str = ";".join(f"{cookie['name']}={cookie['value']}" for cookie in cookies if cookie.get("name", "") in cookies_need)
        authorizev3 = {cookie['name']:cookie['value'] for cookie in cookies if cookie.get("name", "") == "WBTokenV3"}
        authorizev3 = authorizev3["WBTokenV3"]

        conn = None
        try:
            conn = await async_connect_to_database()
            add_set_data_from_db(
                conn=conn,
                table_name="myapp_wblk",
                data=dict(
                    lk_id=inn["id"],
                    cookie=cookies_str,
                    authorizev3=authorizev3
                ),
                conflict_fields=["id"]
            )
        except Exception as e:
            logger.error(f"Ошибка при обновлении кукков: {e}")
        finally:
            if conn:
                await conn.close()

