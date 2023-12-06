import asyncio
import html as hypertext
import re

import dateparser
from httpx import AsyncClient
from parsel import Selector
from rich.console import Console

from .BaseScraper import BaseScraper
from .StorageWorker import StorageWorker
from .utils import normalize_date

SENTINEL = "STOP"

console = Console()


class ExpressDasIlhasScraper(BaseScraper):
    async def parse_page(self, client, page_url):
        try:
            resp = await self.fetch_page(client, page_url)
            html = Selector(text=resp.text)
            urls = html.css(".featuredContent > a.intern::attr(href)").getall()

            console.print(
                f"Found [cyan]{len(urls)}[/cyan] URLs on page [blue]{page_url}[/blue]"
            )

            for url in urls:
                url = f"https://expressodasilhas.cv{url}"
                resp = await self.fetch_page(client, url)
                content = Selector(text=resp.text)
                await self.parse_article(url, content)

            # retrieve last_value from the html page and build data argument for POST request
            pattern = re.compile(r"let last = '([^']*)'")
            match = pattern.search(resp.text)

            if match is not None:
                last_value = match.group(1)

                data = {
                    "listType": "section",
                    "slug": page_url.rsplit("/", 1)[-1],
                    "before": last_value,
                }
                endpoint = "api/lists/section"
                resp = await self.send_post_request(endpoint, data)

                while True:
                    payload = resp.json()

                    await self.parse_json(payload)

                    # Update the before and slug for argument
                    data["before"] = payload.get("last")
                    data["slug"] = payload.get("list")[0].get("slug").split("/")[0]

                    # Send another post request
                    res = await self.send_post_request(endpoint, data)

                    if res is not None:
                        await self.parse_json(res.json())
                    else:
                        break
            else:
                console.print(f"No next page found on [blue]{page_url}[/blue]")

        except Exception as e:
            console.print(f"[red]Error parsing page {page_url}[/red]: {e}")

    async def parse_json(self, payload):
        async with AsyncClient() as client:
            for article_dict in payload.get("list"):
                link = f"{self.base_url}/{article_dict.get('slug')}"
                res = await self.fetch_page(client, link)
                sel = Selector(text=res.text)
                await self.parse_article(link, sel)

    async def parse_article(self, page_url, html):
        try:
            article_block = html.css("div.row.article")
            author_datepub = article_block.css(".topSignature > p")
            author = set(author_datepub.css(".intern.author::text").getall())
            author = ", ".join(map(str, author))

            item = {
                "source": "expressodasilhas",
                "title": article_block.css(".row > h1::text").get(),
                "author": author,
                "date_pub": normalize_date(
                    dateparser.parse(
                        article_block.css(
                            ".col-sm-6.topSignature > p > span::text"
                        ).get(),
                        settings={"TIMEZONE": "UTC-1"},
                    )
                ),
                "link": page_url,
                "topic": article_block.css(".antetitle > a::text").get(),
                "text_html": hypertext.unescape(
                    " <br/> ".join(
                        article_block.css(".content > .summary + .articleText").getall()
                    )
                ),
            }

            # Put the item into the storage queue
            await self.storage_queue.put(item)

        except Exception as e:
            console.print(f"[red]Error parsing article {page_url}[/red]: {e}")


async def main():
    start_urls_santiago = [
        "/politica",
        "/economia",
        "/pais",
        "/mundo",
        "/cultura",
        "/desporto",
        "/empresas-negocios",
        "/eitec",
        "/lifestyle",
        "/opiniao",
    ]

    # Create an async multiprocessing Queue for inter-process communication
    storage_queue_santiago = asyncio.Queue()

    # Start the storage worker processes
    storage_worker_santiago = StorageWorker(
        storage_queue_santiago, "scraper_database.db"
    )

    storage_process_santiago = asyncio.create_task(storage_worker_santiago.run())

    # Start the scraper processes

    scraper_santiago = ExpressDasIlhasScraper(
        base_url="https://expressodasilhas.cv",
        start_urls=start_urls_santiago,
        storage_queue=storage_queue_santiago,
        database_path="scraper_database.db",
    )

    scraper_process_santiago = asyncio.create_task(scraper_santiago.run())

    # Wait for the scraper processes to finish
    await asyncio.gather(scraper_process_santiago)

    # Signal the storage workers to exit
    await storage_queue_santiago.put(SENTINEL)

    # Wait for the storage worker processes to finish
    await storage_process_santiago


if __name__ == "__main__":
    asyncio.run(main())
