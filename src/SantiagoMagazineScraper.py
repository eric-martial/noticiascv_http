import asyncio
import html as hypertext

import dateparser
from parsel import Selector
from rich.console import Console

from .BaseScraper import BaseScraper
from .StorageWorker import StorageWorker
from .utils import normalize_date

SENTINEL = "STOP"

console = Console()


class SantiagoMagazineScraper(BaseScraper):
    async def parse_page(self, client, page_url):
        try:
            resp = await self.fetch_page(client, page_url)
            html = Selector(text=resp.text)
            urls = html.css("h3.title-semibold-dark a::attr(href)").getall()

            console.print(
                f"Found [cyan]{len(urls)}[/cyan] URLs on page [blue]{page_url}[/blue]"
            )

            for url in urls:
                url = f"https://santiagomagazine.cv{url}"
                resp = await self.fetch_page(client, url)
                content = Selector(text=resp.text)
                await self.parse_article(url, content)

            next_page_url = (
                "https://santiagomagazine.cv"
                + html.css(
                    "div.pagination-btn-wrapper li.page-item a::attr(href)"
                ).extract()[-1]
            )
            if next_page_url is not None:
                await self.parse_page(client, next_page_url)
            else:
                console.print(f"No next page found on [blue]{page_url}[/blue]")

        except Exception as e:
            console.print(f"[red]Error parsing page {page_url}[/red]: {e}")

    async def parse_article(self, page_url, html):
        try:
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
            console.print(f"[red]Error parsing article {page_url}[/red]: {e}")


async def main():
    start_urls_santiago = [
        "/economia",
        "/politica",
        "/cultura",
        "/diaspora",
        "/editorial",
        "/outros-mundos",
        "/sociedade",
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
