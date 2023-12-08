import asyncio
import random

from fake_useragent import UserAgent
from httpx import AsyncClient, HTTPStatusError, RequestError

from .scraper_logger import ScraperLogger

SENTINEL = "STOP"


class BaseScraper:
    def __init__(self, base_url, start_urls, storage_queue, database_path):
        self.base_url = base_url
        self.start_urls = start_urls
        self.storage_queue = storage_queue
        self.database_path = database_path
        self.user_agent = UserAgent()
        self.user_agents = [
            self.user_agent.chrome,
            self.user_agent.firefox,
            self.user_agent.edge,
        ]
        self.proxies = {
            "http://": "http://18.230.20.205:3128",
            "http://": "http://139.59.1.14:8080",
            "http://": "http://8.219.43.134:8118",
            "http://": "http://162.223.94.164:80",
            "http://": "http://54.38.181.125:80",
            "http://": "http://162.223.94.163:80",
            "http://": "http://193.15.14.198:80",
            "http://": "http://8.219.169.172:3132",
            "http://": "http://139.162.78.109:3128",
            "http://": "http://20.210.113.32:80",
            "http://": "http://20.24.43.214:80",
            "http://": "http://20.206.106.192:80",
            "http://": "http://47.56.110.204:8989",
            "http://": "http://162.248.225.130:80",
            "http://": "http://162.248.225.11:80",
            "http://": "http://198.199.86.11:3128",
            "http://": "http://52.117.157.155:8002",
            "http://": "http://162.223.116.54:80",
            "http://": "http://162.248.225.198:80",
        }

    async def fetch_page(self, client, page_url, max_retries=5):
        retries = 0

        while retries < max_retries:
            headers = {"User-Agent": random.choice(self.user_agents)}

            try:
                resp = await client.get(page_url, headers=headers, timeout=180)
                resp.raise_for_status()
                return resp
            except HTTPStatusError as e:
                if e.response.status_code == 502:
                    retries += 1
                    ScraperLogger.log_warning(
                        f"{page_url} \n 502 Bad Gateway. Retrying... ({retries}/{max_retries})"
                    )
                    await asyncio.sleep(2**retries)  # Exponential backoff
                else:
                    # If it's not a 502 status, re-raise the exception
                    raise
            except RequestError as e:
                retries += 1
                ScraperLogger.log_warning(
                    f"{page_url} \n  Request failed. Retrying... ({retries}/{max_retries})"
                )
                await asyncio.sleep(2**retries)  # Exponential backoff

        ScraperLogger.log_error(f"Failed to fetch page {page_url} after {max_retries} retries.")

    async def send_post_request(self, endpoint, data):
        url = f"{self.base_url}/{endpoint}"

        async with AsyncClient(proxies=self.proxies) as client:
            try:
                response = await client.post(url, data=data)

                # Check if the request was successful (status code 2xx)
                if response.status_code // 100 == 2:
                    ScraperLogger.log_info(
                        f"POST request to {url} successful! Response: {response.text}"
                    )
                    return response  # Return the response data
                else:
                    ScraperLogger.log_warning(
                        f"POST request to {url} failed with status code {response.status_code}. Response: {response.text}"
                    )
                    return None
            except RequestError as e:
                ScraperLogger.log_error(
                    f"An error occurred during the POST request to {url}: {e}"
                )
                return None

    async def parse_page(self, client, page_url):
        raise NotImplementedError("Subclasses must implement the parse_page method.")

    async def parse_article(self, page_url, html):
        raise NotImplementedError("Subclasses must implement the parse_article method.")

    async def run(self):
        async with AsyncClient(proxies=self.proxies) as client:
            for start_url in self.start_urls:
                full_url = f"{self.base_url}{start_url}"
                await self.parse_page(client, full_url)

        # Signal the storage saver to exit
        await self.storage_queue.put(SENTINEL)
