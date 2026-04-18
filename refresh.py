import asyncio
import json
import logging
import os
import random
import time
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from playwright.sync_api import sync_playwright, ProxySettings
from dotenv import load_dotenv
import re
from asyncio import Lock
from pathlib import Path

load_dotenv()

API_URL = "https://www.idealo.fr/csr/api/v2/modules/dealsResult"
TOTAL_PAGES = 67
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 25
OUTPUT_FILE = "products.jsonl"
PROGRESS_FILE = "progress.json"
PROXY = os.getenv("PROXY")

HEADERS_API = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.idealo.fr/bons-plans",
    "Priority": "u=1, i",
    "Ismobile": "false",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Sessioninfoencrypted": "8438ad615e15693a44e4322cf9729a40c686c3312e6320fc443d6a483451876a5b07be9faa76f513592e65a96e93970a5b835306c2db9a4c9b5d15c65ecd8f26ca757935b1d28c6d6a5125c08090e0c7a6fe34113e83abf4102ada74680ed9333a3a2a34efd6b9e9ce3b102b51b8529e7837e3183fc629fd7b77cd77e6532ff5803623d608360b0873b19e24b75eb6be428c888ac0f8e6ae87f15d91807644a6c0d590504e82f14818eb5e414a56dcb989fc22498c553e563a29248b84c34573d8ea4105eeb755e2bbe12f453a43be45c6fae6392c7149a1e157a978c7f2748255daf12f62cc77d2e423dff9f232689cc1c0c5106f9368a73abdd8fcb12f9dae21032e97d5f0d9f7cc44389ec5385e00e3910d946b3f86bbda257e6880865f783cbaa794042d18eaf948a1184f1138f745f8ea74f74bc36a17f791c5886c6e240e68169653d6f44d4149d7281d3a3fac8c70becef7fedd6a1bc48b2ac0e28ef47ff85ea6a33cf3229d682d208ddd09e96cd44ec934de18b6185e964ab2d646165ff7f5bfd8928a40f210d64e84353b5505a72c16b0fc0632abccadb214588eb5390de7d073200e56261798090921a558fc63d43a17d9cc2d2d0b371c74a91a29a4a754ce2cffe4dae444774cef803c169f289118206dd06ae5d8055a20f3f2170b60596e7f3a422ea35dde072a25dac851979521734265c152cb93cb997eb12c66a44dfe407222b8bbc3adedcf6933b841871e29b7484909aa023b0372a627a3ca060e9302b15e54020b1440aa2cfbedc1ba196775943df6038dcbf23030ff88f657f87fc0d8c15a4978796551ceaba1c9a26d778ed7291d9b10bc318281bcdb"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
file_lock = Lock()
cookie_lock = Lock()
last_cookie_time = 0
cookie_cache: Optional[str] = None


def load_progress() -> Dict[str, Any]:
    try:
        if Path(PROGRESS_FILE).exists():
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logging.warning(e)
    return {"processed_urls": [], "collected_products": 0}


def save_progress(processed_urls: List[str], collected_products: int):
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "processed_urls": processed_urls,
                "collected_products": collected_products,
                "last_updated": time.time()
            }, f)
    except Exception as e:
        logging.warning(e)


def get_cookie(proxy: Optional[str]) -> Optional[str]:
    proxy_settings: ProxySettings | None = None
    if proxy:
        try:
            parts = proxy.replace("http://", "").split("@")
            if len(parts) == 2:
                creds, server = parts
                username, password = creds.split(":")
                proxy_settings: ProxySettings = {"server": server, "username": username, "password": password}
        except Exception as e:
            logging.warning(e)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"], proxy=proxy_settings)
            context = browser.new_context()
            page = context.new_page()
            page.goto("https://www.idealo.fr", wait_until="load", timeout=60000)
            cookies = context.cookies()
            browser.close()
            return "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    except Exception as e:
        logging.warning(e)
        return None


async def refresh_cookie():
    global cookie_cache, last_cookie_time
    async with cookie_lock:
        if time.time() - last_cookie_time < 60 and cookie_cache:
            return cookie_cache
        cookie = await asyncio.to_thread(get_cookie, PROXY)
        if cookie:
            cookie_cache = cookie
            last_cookie_time = time.time()
        return cookie_cache


async def resolve_hash(session: AsyncSession, token: str) -> Optional[str]:
    for attempt in range(MAX_RETRIES):
        try:
            r = await session.post(
                "https://www.idealo.fr/ipc/prg",
                data={"value": token},
                timeout=60,
                proxy=PROXY
            )
            if r.status_code == 200:
                return str(r.url)
        except Exception as e:
            logging.warning(e)
        await asyncio.sleep(2 ** attempt)
    return None


async def fetch_urls(session: AsyncSession, page_index: int) -> List[str]:
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                r = await session.get(
                    API_URL,
                    params={
                        "locale": "fr_FR",
                        "pageIndex": page_index,
                        "itemsPerPage": 60,
                        "itemStates": "BARGAIN"
                    },
                    timeout=60,
                    proxy=PROXY
                )
                r.raise_for_status()
                data = r.json()
                items = data.get("items", [])

                logging.info(f"Page {page_index}: {len(items)} items")

                urls, hashes = [], []

                for item in items:
                    href = item.get("href")
                    if not href:
                        continue
                    if href.startswith("http"):
                        urls.append(href)
                    else:
                        hashes.append(href)

                if hashes:
                    resolved = await asyncio.gather(*[resolve_hash(session, h) for h in hashes])
                    urls.extend([u for u in resolved if u])

                return urls

            except Exception as e:
                logging.warning(e)
                await asyncio.sleep(2 ** attempt)

        return []


async def collect_urls() -> List[str]:
    params = {"allow_redirects": True, "timeout": 60}
    async with AsyncSession(headers=HEADERS_API, impersonate="chrome142", http_version="v2", **params) as session:
        tasks = [fetch_urls(session, i) for i in range(TOTAL_PAGES)]
        results = await asyncio.gather(*tasks)

    all_urls = [u for batch in results for u in batch]

    unique_urls = list(dict.fromkeys(all_urls))
    logging.info(f"Collected URLs: {len(unique_urls)} (raw: {len(all_urls)})")

    return unique_urls


class IDEALOScraper:
    def __init__(self, urls: List[str], headers: dict):
        self.headers = headers
        self.url_queue = asyncio.Queue()
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        self.progress = load_progress()
        self.progress_lock = Lock()
        self.seen_lock = Lock()

        processed = set(self.progress.get("processed_urls", []))
        self.processed_urls = list(processed)
        self.collected_count = self.progress.get("collected_products", 0)

        self.remaining_urls = [u for u in urls if u not in processed]

        self.seen_products = set()

    async def fetch(self, session: AsyncSession, url: str) -> Optional[str]:
        for attempt in range(MAX_RETRIES):
            try:
                async with self.semaphore:
                    await asyncio.sleep(random.uniform(0.2, 0.6))
                    r = await session.get(url, headers=self.headers, timeout=45, proxy=PROXY)

                    if r.status_code in (401, 403):
                        cookie = await refresh_cookie()
                        if cookie:
                            self.headers["Cookie"] = cookie
                        continue

                    if r.status_code < 400:
                        return r.text

            except Exception as e:
                logging.warning(e)

            await asyncio.sleep(2 ** attempt)

        return None

    @staticmethod
    def parse(html: str) -> Optional[Dict[str, Any]]:
        try:
            soup = BeautifulSoup(html, "lxml")

            price_el = soup.find("div", attrs={"class": "productOffers-listItemOfferShippingDetails"})
            price = None

            if price_el:
                price_clean = price_el.text.strip().replace("€ livraison incl.", "")
                price_clean = re.sub(r"[^\d.,]", "", price_clean).replace(",", ".")
                if price_clean:
                    price = float(price_clean)

            scripts = soup.find_all("script", type="application/ld+json")

            for s in scripts:
                if not s.string:
                    continue

                data = json.loads(str(s.string).strip())

                if isinstance(data, dict) and "offers" in data:
                    name = data.get("name")

                    if name and price is not None:
                        return {
                            "product_name": name,
                            "supplier_price": price,
                            "product_gtin": data.get("gtin", ""),
                            "product_url": data.get("url", "")
                        }

        except Exception as e:
            logging.warning(e)

        return None

    async def worker(self, session: AsyncSession):
        while True:
            url = await self.url_queue.get()

            if url is None:
                self.url_queue.task_done()
                break

            try:
                html = await self.fetch(session, url)

                async with self.progress_lock:
                    self.processed_urls.append(url)

                if html:
                    data = self.parse(html)

                    if data:
                        url = data.get("product_url", "")
                    
                        match = re.search(r"/prix/(\d+)", url)
                        product_id = match.group(1) if match else None
                    
                        key = product_id or data.get("product_gtin") or url
                    
                        if key:
                            async with self.seen_lock:
                                if key in self.seen_products:
                                    continue
                                self.seen_products.add(key)
                    
                            await self.data_queue.put(data)
                    
                            async with self.progress_lock:
                                self.collected_count += 1

                if len(self.processed_urls) % 50 == 0:
                    async with self.progress_lock:
                        save_progress(self.processed_urls, self.collected_count)

            finally:
                self.url_queue.task_done()

    async def saver(self):
        while True:
            item = await self.data_queue.get()

            if item is None:
                self.data_queue.task_done()
                break

            async with file_lock:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

            self.data_queue.task_done()

    async def run(self):
        params = {"allow_redirects": True, "timeout": 60}

        async with AsyncSession(impersonate="chrome142", http_version="v2", **params) as session:
            for url in self.remaining_urls:
                await self.url_queue.put(url)

            workers = [asyncio.create_task(self.worker(session)) for _ in range(CONCURRENT_REQUESTS)]
            saver_task = asyncio.create_task(self.saver())

            await self.url_queue.join()

            for _ in workers:
                await self.url_queue.put(None)

            await asyncio.gather(*workers)

            await self.data_queue.put(None)
            await self.data_queue.join()
            await saver_task

            async with self.progress_lock:
                save_progress(self.processed_urls, self.collected_count)


def main():
    urls = asyncio.run(collect_urls())

    cookie = get_cookie(PROXY) or ""

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookie,
        "Priority": "u=0, i",
        "Sec-Ch-Ua": '"Chromium";v="142", "Not-A.Brand";v="24", "Google Chrome";v="142"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    }

    scraper = IDEALOScraper(urls, headers)
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
