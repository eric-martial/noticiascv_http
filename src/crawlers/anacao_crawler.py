import asyncio
import html as hypertext

import aiosqlite
import dateparser
from icecream import ic
from parsel import Selector
from rich.console import Console

from ..base_scraper import BaseScraper
from ..scraper_logger import ScraperLogger
from ..storage_worker import StorageWorker
from ..utils import normalize_date

SENTINEL = "STOP"

console = Console()


class AnacaoScraper(BaseScraper):
    def __init__(self, base_url, start_urls, storage_queue, database_path):
        super().__init__(base_url, start_urls, storage_queue, database_path)
        self.processed_urls = set()

    async def load_processed_urls(self):
        async with aiosqlite.connect(self.database_path) as conn:
            cursor = await conn.cursor()
            await cursor.execute("SELECT link FROM articles where source = 'anacao'")
            rows = await cursor.fetchall()
            self.processed_urls = set(row[0] for row in rows)

    async def parse_page(self, client, page_url):
        try:
            await self.load_processed_urls()

            ScraperLogger.log_info(f"Parsing page: {page_url}")
            resp = await self.fetch_page(client, page_url)
            html = Selector(text=resp.text)

            featured_urls = html.css("div#feat-top-wrap a::attr(href)").getall()
            urls = (
                featured_urls
                + html.css("div#archive-list-wrap li>a::attr(href)").getall()
            )

            ScraperLogger.log_info(
                f"Found [cyan]{len(urls)}[/cyan] URLs on page [blue]{page_url}[/blue]"
            )

            for url in urls:
                if url not in self.processed_urls:
                    ScraperLogger.log_info(f"Parsing article: {url}")
                    resp = await self.fetch_page(client, url)
                    content = Selector(text=resp.text)
                    await self.parse_article(url, content)
                    self.processed_urls.add(url)
                else:
                    ScraperLogger.log_info(f"Skipped existing article: {url}")

            pagination_links = html.css("div.pagination a::attr(href)").getall()
            if pagination_links:
                next_page_url = pagination_links[-2]
                await self.parse_page(client, next_page_url)
            else:
                ScraperLogger.log_info(f"No next page found on [blue]{page_url}[/blue]")

        except Exception as e:
            ScraperLogger.log_error(f"Error parsing page {page_url}: {e}")

    async def parse_article(self, page_url, html):
        try:
            ScraperLogger.log_info(f"Parsing article: {page_url}")
            article_css_selector = """
                div#content-main p,
                div[dir="auto"] *::text,
                div.news-details-layout1 p *::text    
            """

            item = {
                "source": "anacao",
                "title": html.css("header#post-header h1::text").get(),
                "author": html.css("header#post-header span.author-name a::text").get(),
                "date_pub": normalize_date(
                    dateparser.parse(html.css("time.post-date::text").get())
                ),
                "link": page_url,
                "topic": html.css("header#post-header span::text").get(),
                "text_html": " <br/> ".join(html.css(article_css_selector).getall()),
            }

            # Put the item into the storage queue
            await self.storage_queue.put(item)

        except Exception as e:
            ScraperLogger.log_error(f"Error parsing article {page_url}: {e}")


async def main():
    start_urls_anacao = [
        "/categoria/sociedade/",
        "/categoria/politica/",
        "/categoria/cultura/",
        "/categoria/economia/",
        "/categoria/desporto/",
        "/categoria/mundo/",
        "/categoria/diaspora/",
        "/categoria/opiniao/",
    ]

    # Create an async multiprocessing Queue for inter-process communication
    storage_queue_anacao = asyncio.Queue()

    # Start the storage worker processes
    storage_worker_anacao = StorageWorker(storage_queue_anacao, "scraper_database.db")

    storage_process_anacao = asyncio.create_task(storage_worker_anacao.run())

    # Start the scraper processes
    scraper_anacao = AnacaoScraper(
        base_url="https://www.anacao.cv",
        start_urls=start_urls_anacao,
        storage_queue=storage_queue_anacao,
        database_path="scraper_database.db",
    )

    scraper_process_anacao = asyncio.create_task(scraper_anacao.run())

    # Wait for the scraper processes to finish
    await asyncio.gather(scraper_process_anacao)

    # Signal the storage workers to exit
    await storage_queue_anacao.put(SENTINEL)

    # Wait for the storage worker processes to finish
    await storage_process_anacao


if __name__ == "__main__":
    asyncio.run(main())
