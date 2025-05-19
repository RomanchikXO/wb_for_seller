import math
import time

import lxml.html
from playwright.async_api import async_playwright
import asyncio
from database.DataBase import async_connect_to_database
import logging
from context_logger import ContextLogger
from playwright_utils import ask_user_for_input
from BOT.states import get_status


logger = ContextLogger(logging.getLogger("wallet_discount_updater"))


async def login_and_get_context():
    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в login_and_get_context")
        return
    try:
        request = ("SELECT phone_number, tg_id "
                   "FROM myapp_customuser "
                   "WHERE groups_id = 1 AND id = 1")
        all_fields = await conn.fetch(request)
        result = [{"number": row["phone_number"], "tg_id": int(row["tg_id"])} for row in all_fields]
        result = result[0]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_customuser. Запрос {request}. Error: {e}")
    finally:
        await conn.close()

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
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
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        # Стандартный пользовательский агент
        locale="ru-RU",  # Устанавливаем локаль
        geolocation={"latitude": 55.7558, "longitude": 37.6173},  # Геолокация Москвы
        permissions=["geolocation"],  # Разрешаем использование геолокации
    )

    page = await context.new_page()

    await page.goto("https://www.wildberries.ru/security/login?returnUrl=https%3A%2F%2Fwww.wildberries.ru%2F")

    # Ожидание появления инпута и ввод номера
    await page.wait_for_selector("input.input-item[inputmode='tel']", timeout=30000)
    input_selector = "input.input-item[inputmode='tel']"
    await page.click(input_selector)  # сфокусировать
    await page.fill(input_selector, "")  # очистить на всякий случай
    await page.type(input_selector, f"+7{result['number']}", delay=100)  # имитируем ручной ввод


    # Ожидание активности кнопки и клик
    await page.wait_for_selector("button#requestCode:not([disabled])", timeout=10000)
    await page.click("button#requestCode")

    # Ожидание появления полей для ввода кода
    await page.wait_for_selector("input.char-input__item.j-b-charinput", timeout=10000)

    # Ввод кода из консоли
    ask_user_for_input(result['tg_id'])

    sms_code = None
    while not sms_code:
        status = get_status(result["tg_id"])
        if status and status.startswith("code_"):
            sms_code = str(status.replace("code_", ""))
            break
        await asyncio.sleep(10)

    # Получение всех инпутов и заполнение их по одной цифре
    inputs = await page.query_selector_all("input.char-input__item.j-b-charinput")
    for i, digit in enumerate(sms_code):
        if i < len(inputs):
            await inputs[i].fill(digit)

    # (Опционально) можно дождаться редиректа или подтверждения входа
    await page.wait_for_timeout(5000)  # или ждём элемент, указывающий на успешный вход

    # Шаг 3: Переход на страницу продавца
    await page.goto("https://www.wildberries.ru/seller/1209217", wait_until="domcontentloaded")

    while True:
        # Зацикливаем с паузой в 5 минут

        # Шаг 4: Поиск первого блока карточки товара
        await page.wait_for_selector("div.product-card__wrapper", timeout=10000)
        first_card = await page.query_selector("div.product-card__wrapper")

        if not first_card:
            print("Карточка товара не найдена.")
            await browser.close()
            return

        # Шаг 5: Извлечение HTML и вывод
        card_html = await first_card.inner_html()

        await set_wallet_discount(card_html)

        await asyncio.sleep(300)
        await page.reload()


async def set_wallet_discount(data):

    a = lxml.html.fromstring(data)

    nmid_block = a.cssselect("a.product-card__link")[0].attrib["href"]
    nmid = int(nmid_block.split("/")[-2])
    red_price = int(a.cssselect("ins")[0].text_content().replace("\xa0", "").replace("₽", ""))

    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в set_wallet_discount")
        return
    try:
        request = ("SELECT blackprice "
                   "FROM myapp_price "
                   "WHERE nmid = $1")
        all_fields = await conn.fetch(request, nmid)
        result = [{"blackprice": int(row["blackprice"])} for row in all_fields]
        result = result[0]
    except Exception as e:
        logger.error(f"Ошибка получения данных из myapp_price. Запрос {request}. Error: {e}")
    finally:
        await conn.close()

    discount = math.floor(100 - (red_price / (result["blackprice"] / 100)))

    conn = await async_connect_to_database()
    if not conn:
        logger.warning(f"Ошибка подключения к БД в set_wallet_discount")
        return
    try:
        request = ("UPDATE myapp_price "
                   "SET wallet_discount = $1")
        await conn.execute(request, discount)
    except Exception as e:
        logger.error(f"Ошибка обновления wallet_discount в myapp_price. Запрос {request}. Error: {e}")
    finally:
        await conn.close()


# loop = asyncio.get_event_loop()
# res = loop.run_until_complete(login_and_get_context())