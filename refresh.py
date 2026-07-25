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
    "Sessioninfoencrypted": "22144298fa4198e42ca26551abba86841f92de4a149a4ecc41d6099a8041ff60548e6e479711a4ddfcc08fda5c18dddbbb68e8985a3825d9f94ed2a5694d6af76b77ea70daab1bf5518902ecb58aaece95ec7bd54ce7a2bdd5dad2423d6531bba900dde3e69c8bd42fcbc657c17a54ba875135b36dce55fd3c13a2b18291361ec09f66d76bc9850c445a21768951288e63fcd40ec1ed5d1895736c3429da9164fc309f12aa98c15fb879298bd667e603fe51595bd63dab15af8baedab190599ebf7c49051a81fa3b14aff44e01e3c91c13c7b4d3cbf68238cb6eb29a9d323f40472618c1e1e8bbdda24117cc1502ce14e79f6200f645f32239ee8678bcdaf9bc0ff582216b467708e1dd8bf41f6ad3110504663962f741ac14ffbec8c93aa02c1b7bf75935f589c04e7ee3cc9ae858f1fc21dbd2fcad4b35aac05f4eb81a0bcd72223fb3cffdcacb120274554e212d2abf56d811edf4bfa2264b6367a8802ca80aae1e92c47008441161fcc2f50742e2cd2b1bbc06005bf949eb9b1dabfd2f1f1609929307f6f6b971e2f68706ab8d8239bfdc139ae8514ae81c14e65954e5b12e1842d69ab8332d01ce314ff9bad49ca4a932652a0d9729b4c6ddf6fcf332bd4706f9f010bf122b76534c439aeba4d99f0d5a20ca88b138e0e00b70e5250a27f21769cc64727508c7d90fdf56e3f7a0049aad314cf8ad1a702226e6e0aad5de568eeba967582cd24d57db46acc0925b86812e9d1002973dcf1236d454cf60b4d10ffaeead75dcbf0879073797caaac9b3e991e81a0a5c65097b6d8bb5faac7968c399fbdb6d30aa5bdbf7de3df04b296d9fd6ca43ed39ad5774eae92c249aa059544468e8b955ffbebd3699c3d70650c8af11620ebbb6358225d3cfe4c76d6fd286ac5cdfd224f3ec89c41b184b3cd11378f0cbe547b214a4b1f4b6b4146f3c89ae234a122a0728047c2754fae148bce289af7b45ce3f2759e4b7d8428a9a50d81769ad77c3ead9f16b80d91b931aa49c5afc216b84ab3ac7aafe81537aa680888ae50c2ea410af7aba36a72008ec4c5e4600dbead817b60110b8e53ea2fc5a",
    "X-Growthbook-Experiments": {"features":{}},
    "X-Growthbook-Previews": {"features":{"sam-3517_2":{"variant":"","source":"defaultValue"},"sam-3628":{"variant":"","source":"defaultValue"},"sam-4104":{"variant":"","source":"defaultValue"},"sam-4208":{"variant":"","source":"defaultValue"},"sam-4224":{"variant":"","source":"defaultValue"},"sam-4547":{"variant":"","source":"defaultValue"},"sam-4727":{"variant":"","source":"defaultValue"},"sam-4746":{"variant":"","source":"defaultValue"},"sam-4749":{"variant":"","source":"defaultValue"},"sam-4764":{"variant":"","source":"defaultValue"},"sam-4771":{"variant":"","source":"defaultValue"},"sam-4816":{"variant":"B","source":"force"},"sam-4889":{"variant":"","source":"defaultValue"},"sam-4953":{"variant":"","source":"defaultValue"},"xp-1039":{"variant":"A","source":"defaultValue"},"zonk-3905":{"variant":"A","source":"defaultValue"},"zonk-3920":{"variant":"A","source":"defaultValue"},"zonk-3954":{"variant":"A","source":"defaultValue"},"zonk-4059":{"variant":"A","source":"defaultValue"},"zonk-4092":{"variant":"OFF","source":"defaultValue"},"zonk-4097":{"variant":"A","source":"defaultValue"},"zonk-4112":{"variant":"A","source":"defaultValue"},"zonk-4262":{"variant":"A","source":"defaultValue"},"zonk-4268":{"variant":"A","source":"defaultValue"},"zonk-4466":{"variant":"A","source":"defaultValue"},"zonk-4536":{"variant":"A","source":"defaultValue"},"zonk-4537":{"variant":"A","source":"defaultValue"},"zonk-4575":{"variant":"A","source":"defaultValue"},"zonk-4576":{"variant":"A","source":"defaultValue"},"zonk-4629":{"variant":"A","source":"defaultValue"},"zonk-4774":{"variant":"A","source":"defaultValue"},"zonk-4870":{"variant":"A","source":"defaultValue"}}},
    "X-Idealo-Renderer-Routing-Secret-V2": "",
    "X-Zonk-Dev": ""
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
        pass
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
        pass


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
            proxy_settings = None
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
            r = await session.post("https://www.idealo.fr/ipc/prg", data={"value": token}, timeout=60, proxy=PROXY)
            if r.status_code == 200:
                return str(r.url)
        except Exception as e:
            logging.warning(e)
            pass
        await asyncio.sleep(2 ** attempt)
    return None


async def fetch_urls(session: AsyncSession, page_index: int) -> List[str]:
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                await asyncio.sleep(random.uniform(0.3, 1.0))
                r = await session.get(API_URL, params={"locale": "fr_FR", "pageIndex": page_index, "itemsPerPage": 60, "itemStates": "BARGAIN"}, timeout=60, proxy=PROXY)
                r.raise_for_status()
                data = r.json()
                items = data.get("items", [])
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
    return list(dict.fromkeys(all_urls))


class IDEALOScraper:
    def __init__(self, urls: List[str], headers: dict):
        self.headers = headers
        self.url_queue = asyncio.Queue()
        self.data_queue = asyncio.Queue(maxsize=1000)
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        self.progress = load_progress()
        self.progress_lock = Lock()
        processed = set(self.progress.get("processed_urls", []))
        self.processed_urls = list(processed)
        self.collected_count = self.progress.get("collected_products", 0)
        self.remaining_urls = [u for u in urls if u not in processed]

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
                pass
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
                price_clean = re.sub(r"[^\d.,]", "", price_clean)
                price_clean = price_clean.replace(",", ".")
                if price_clean:
                    price = float(price_clean)

            scripts = soup.find_all("script", type="application/ld+json")
            for s in scripts:
                if not s.string:
                    continue
                data = json.loads(s.string.strip())
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
