import asyncio
import html as hypertext
import re
import traceback

import aiosqlite
import dateparser
from httpx import AsyncClient
from parsel import Selector

from ..base_scraper import BaseScraper
from ..scraper_logger import ScraperLogger
from ..storage_worker import StorageWorker
from ..utils import normalize_date

traceback.print_exc()

SENTINEL = "STOP"


class ExpressDasIlhasScraper(BaseScraper):
    def __init__(self, base_url, start_urls, storage_queue, database_path):
        super().__init__(base_url, start_urls, storage_queue, database_path)
        self.processed_urls = set()

    async def load_processed_urls(self):
        async with aiosqlite.connect(self.database_path) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                "SELECT link FROM articles where source = 'expressodasilhas'"
            )
            rows = await cursor.fetchall()
            self.processed_urls = set(row[0] for row in rows)

    async def parse_page(self, client, page_url):
        try:
            await self.load_processed_urls()

            ScraperLogger.log_info(f"Parsing page: {page_url}")
            resp = await self.fetch_page(client, page_url)
            html = Selector(text=resp.text)
            urls = html.css(".featuredContent > a.intern::attr(href)").getall()

            ScraperLogger.log_info(
                f"Found [cyan]{len(urls)}[/cyan] URLs on page [blue]{page_url}[/blue]"
            )

            for url in urls:
                url = f"https://expressodasilhas.cv{url}"

                if url not in self.processed_urls:
                    page = await self.fetch_page(client, url)
                    content = Selector(text=page.text)
                    await self.parse_article(url, content)
                    self.processed_urls.add(url)
                else:
                    ScraperLogger.log_info(f"Skipped existing URL: {url}")

            pattern = re.compile(r"let last = '([^']*)'")
            match = pattern.search(resp.text)

            if match is not None:
                last_value = match.group(1)

                form_data = {
                    "listType": "section",
                    "slug": page_url.rsplit("/", 1)[-1],
                    "before": last_value,
                }
                endpoint = "api/lists/section"
                response = await self.send_post_request(endpoint, form_data)
                payload = response.json()
                await self.parse_json(payload)

                while "last" in payload:
                    form_data["before"] = payload.get("last")
                    form_data["slug"] = payload.get("list")[0].get("slug").split("/")[0]
                    res = await self.send_post_request(endpoint, form_data)
                    payload = res.json()
                    await self.parse_json(payload)
            else:
                ScraperLogger.log_info(
                    f"No Load More button found on [yellow]{page_url}[/yellow]"
                )

        except Exception as e:
            ScraperLogger.log_error(f"Error parsing page {page_url}: {e}")

    async def parse_json(self, payload):
        await self.load_processed_urls()

        async with AsyncClient() as client:
            for article_dict in payload.get("list"):
                link = f"{self.base_url}/{article_dict.get('slug')}"

                if link not in self.processed_urls:
                    res = await self.fetch_page(client, link)
                    sel = Selector(text=res.text)
                    await self.parse_article(link, sel)
                else:
                    ScraperLogger.log_info(f"Skipped existing URL: {link}")

    async def parse_article(self, page_url, html):
        try:
            ScraperLogger.log_info(f"Parsing article: {page_url}")
            article_block = html.css("div.row.article")
            author_datepub = article_block.css(".topSignature > p")
            author = set(author_datepub.css(".intern.author::text").getall())
            author = ", ".join(map(str, author))
            article_css_selector = """
                div.content p::text,
                div.summary::text,
                div.articleText *::text,
                div.content div[style="text-align: justify;"] *::text
            """

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
                    " <br/> ".join(article_block.css(article_css_selector).getall())
                ),
            }

            # Put the item into the storage queue
            await self.storage_queue.put(item)

        except Exception as e:
            ScraperLogger.log_error(f"Error parsing article {page_url}: {e}")


async def main():
    start_urls_xdi = (
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
    )

    # Create an async multiprocessing Queue for inter-process communication
    storage_queue_xdi = asyncio.Queue()

    # Start the storage worker processes
    storage_worker_xdi = StorageWorker(
        storage_queue_xdi, "scraper_database.db"
    )

    storage_process_xdi = asyncio.create_task(storage_worker_xdi.run())

    # Start the scraper processes
    scraper_xdi = ExpressDasIlhasScraper(
        base_url="https://expressodasilhas.cv",
        start_urls=start_urls_xdi,
        storage_queue=storage_queue_xdi,
        database_path="scraper_database.db",
    )

    scraper_process_xdi = asyncio.create_task(scraper_xdi.run())

    # Wait for the scraper processes to finish
    await asyncio.gather(scraper_process_xdi)

    # Signal the storage workers to exit
    await storage_queue_xdi.put(SENTINEL)

    # Wait for the storage worker processes to finish
    await storage_process_xdi


if __name__ == "__main__":
    asyncio.run(main())
