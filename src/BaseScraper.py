import asyncio

from httpx import AsyncClient, RequestError
from rich.console import Console

SENTINEL = "STOP"

console = Console()


class BaseScraper:
    def __init__(self, base_url, start_urls, storage_queue, database_path):
        self.base_url = base_url
        self.start_urls = start_urls
        self.storage_queue = storage_queue
        self.database_path = database_path

    async def fetch_page(self, client, page_url, max_retries=5):
        retries = 0
        while retries < max_retries:
            try:
                resp = await client.get(page_url, timeout=180)
                resp.raise_for_status()
                return resp
            except RequestError as e:
                retries += 1
                print(f"Request failed. Retrying... ({retries}/{max_retries})")
                await asyncio.sleep(2**retries)  # Exponential backoff
        raise RuntimeError(f"Failed to fetch page after {max_retries} retries.")

    async def send_post_request(self, endpoint, data):
        url = f"{self.base_url}/{endpoint}"

        async with AsyncClient() as client:
            try:
                response = await client.post(url, data=data)

                # Check if the request was successful (status code 2xx)
                if response.status_code // 100 == 2:
                    print(f"POST request to {url} successful!")
                    print("Response:", response.text)
                    return response.text  # Return the response data
                else:
                    print(f"POST request to {url} failed with status code {response.status_code}")
                    print("Response:", response.text)
                    return None
            except RequestError as e:
                print(f"An error occurred during the POST request to {url}: {e}")
                return None

    async def parse_page(self, client, page_url):
        raise NotImplementedError("Subclasses must implement the parse_page method.")

    async def parse_article(self, page_url, html):
        raise NotImplementedError("Subclasses must implement the parse_article method.")

    async def run(self):
        async with AsyncClient() as client:
            for start_url in self.start_urls:
                full_url = f"{self.base_url}{start_url}"
                await self.parse_page(client, full_url)

        # Signal the storage saver to exit
        await self.storage_queue.put(SENTINEL)
