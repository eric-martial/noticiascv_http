import asyncio
import html as hypertext
import aiosqlite
import dateparser
from parsel import Selector
from .base_scraper import BaseScraper
from .storage_worker import StorageWorker
from .utils import normalize_date
from .scraper_logger import ScraperLogger
from icecream import ic

SENTINEL = "STOP"

class SantiagoMagazineScraper(BaseScraper):
    def __init__(self, base_url, start_urls, storage_queue, database_path):
        super().__init__(base_url, start_urls, storage_queue, database_path)
        self.processed_urls = set()

    async def load_processed_urls(self):
        async with aiosqlite.connect(self.database_path) as conn:
            cursor = await conn.cursor()
            await cursor.execute("SELECT link FROM articles where source = 'santiagomagazine'")
            rows = await cursor.fetchall()
            self.processed_urls = set(row[0] for row in rows)
            
    async def parse_page(self, client, page_url):
        try:
            await self.load_processed_urls()
            
            ScraperLogger.log_info(f"Parsing page: {page_url}")
            resp = await self.fetch_page(client, page_url)
            html = Selector(text=resp.text)
            urls = html.css("h3.title-semibold-dark a::attr(href)").getall()

            ScraperLogger.log_info(
                f"Found [cyan]{len(urls)}[/cyan] URLs on page [blue]{page_url}[/blue]"
            )

            for url in urls:
                url = f"https://santiagomagazine.cv{url}"

                if url not in self.processed_urls:
                    resp = await self.fetch_page(client, url)
                    content = Selector(text=resp.text)
                    await self.parse_article(url, content)
                    self.processed_urls.add(url)
                else:
                    ScraperLogger.log_info(f"Skipped existing URL: {url}")

            next_page_link = html.css("li.page-item.active + li.page-item a::attr(href)").get()
            next_page_url = f"https://santiagomagazine.cv{next_page_link}" if next_page_link else None

            if next_page_url is not None and next_page_url != page_url:
                await self.parse_page(client, next_page_url)
            else:
                ScraperLogger.log_info(f"No next page found on [blue]{page_url}[/blue]")
        
        except Exception as e:
            ScraperLogger.log_error(f"Error parsing page {page_url}: {e}")

    async def parse_article(self, page_url, html):
        try:
            ScraperLogger.log_info(f"Parsing article: {page_url}")
            article_block = html.css("div.news-details-layout1")
            article_publication = html.css(
                "div.news-details-layout1 ul.post-info-dark>li>a::text"
            ).getall()

            item = {
                "source": "santiagomagazine",
                "title": article_block.css("h2.title-semibold-dark::text").get(),
                "author": article_publication[-3],
                "date_pub": normalize_date(
                    dateparser.parse(
                        article_publication[-1], settings={"TIMEZONE": "UTC-1"}
                    )
                ),
                "link": page_url,
                "topic": article_block.css("div.topic-box-sm::text").get(),
                "text_html": hypertext.unescape(
                    " <br/> ".join(
                        article_block.css("blockquote::text,p::text").getall()
                    )
                ),
            }

            # Put the item into the storage queue
            await self.storage_queue.put(item)

        except Exception as e:
            ScraperLogger.log_error(f"Error parsing article {page_url}: {e}")


async def main():
    start_urls_santiago = [
        "/economia",
        "/politica",
        "/cultura",
        "/diaspora",
        "/editorial",
        "/outros-mundos",
        "/sociedade",
        "/ambiente",
        "/boas-novas",
        "/colunista",
        "/desporto",
        "/elas",
        "/entrelinhas",
        "/entrevista",
        "/estaticos",
        "/ponto-de-vista",
        "/publireportagem",
        "/regioes",
        "/tecnologia",
    ]

    # Create an async multiprocessing Queue for inter-process communication
    storage_queue_santiago = asyncio.Queue()

    # Start the storage worker processes
    storage_worker_santiago = StorageWorker(
        storage_queue_santiago, "scraper_database.db"
    )

    storage_process_santiago = asyncio.create_task(storage_worker_santiago.run())

    # Start the scraper processes
    scraper_santiago = SantiagoMagazineScraper(
        base_url="https://santiagomagazine.cv",
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
