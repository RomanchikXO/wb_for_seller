import random
import httpx
from typing import Union
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
    "Content-Type": "application/json; charset=UTF-8",
}


def generate_random_user_agent() -> str:
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
    platforms = [
        "Windows NT 10.0",
        "Windows NT 6.1",
        "Macintosh; Intel Mac OS X 10_15_7",
        "X11; Linux x86_64",
    ]
    versions = [
        lambda: f"{random.randint(70, 110)}.0.{random.randint(0, 9999)}.{random.randint(0, 150)}",
        lambda: f"{random.randint(70, 110)}.0.{random.randint(0, 9999)}",
    ]
    browser = random.choice(browsers)
    platform = random.choice(platforms)
    version = random.choice(versions)()
    return f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) {browser}/{version} Safari/537.36"


def get_data(method: str, url: str, i: int, response_type="json", **kwargs):
    attempt, max_attemps = 0, 4
    if headers := kwargs.pop("headers"):
        headers["User-Agent"] = generate_random_user_agent()
    while attempt <= max_attemps:
        time.sleep(5)
        # proxies = {"http://": f"http://{random.choice(proxies_all)}"}
        timeout = httpx.Timeout(10.0, connect=5.0)
        try:
            with httpx.Client(
                timeout=timeout,
                # proxies=proxies
            ) as client:
                response = client.request(
                    method.upper(), url, headers=headers, **kwargs
                )
            if response.status_code == 404:
                print(f"Bad link 404. {url}")
                return None
            if response_type == "json":
                result = response.json()
            elif response_type == "text":
                result = response.text
            return result
        except:
            attempt += 1
            print(f"Can't get data, retry {attempt}")
            time.sleep(attempt * 2)


def calculate_card_price(price: Union[int, float]) -> int:
    card_price = int((price * 0.97))
    if price >= 15000:
        card_price = 0
    return card_price


def parse_link(
    link: Union[int, str],
):
    api_url = "https://card.wb.ru/cards/detail"
    params = {
        "spp": "0",
        "reg": "0",
        "appType": "1",
        "emp": "0",
        "dest": -4734876,
        "nm": link,
    }
    data = get_data("get", api_url, 0, "json", headers=headers, params=params)

    if not data or not data["data"]["products"]:
        print(f"Fail {link}")
        return

    sku = data["data"]["products"][0]

    price = sku.get("priceU", 0) / 100
    promo_price = sku.get("salePriceU", 0) / 100
    if price == promo_price:
        promo_price = 0
    if promo_price > price:
        prices = [price, promo_price]
        price = prices[1]
        promo_price = prices[0]

    card_price = calculate_card_price(promo_price) if promo_price else calculate_card_price(price)
    return card_price

def safe_parse_link(link):
    try:
        data = parse_link(link)
        return data
    except Exception as e:
        print(f"Can't parse link. Url: {link}. Error: {e}")

def parse_by_links(links: list) -> list:
    tasks = [
        safe_parse_link(link)
        for link in links
    ]
    return tasks


def parse(links: list) -> list:
    response = parse_by_links(links)
    return response
