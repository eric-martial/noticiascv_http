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


class AnacaoScraper(BaseScraper):
    async def parse_page(self, client, page_url):
        try:
            resp = await self.fetch_page(client, page_url)
            html = Selector(text=resp.text)

            featured_urls = html.css("div#feat-top-wrap a::attr(href)").getall()
            urls = (
                featured_urls
                + html.css("div#archive-list-wrap li>a::attr(href)").getall()
            )

            console.print(
                f"Found [cyan]{len(urls)}[/cyan] URLs on page [blue]{page_url}[/blue]"
            )

            for url in urls:
                resp = await self.fetch_page(client, url)
                content = Selector(text=resp.text)
                await self.parse_article(url, content)

            pagination_links = html.css("div.pagination a::attr(href)").getall()
            if pagination_links:
                next_page_url = pagination_links[-2]
                await self.parse_page(client, next_page_url)
            else:
                console.print(f"No next page found on [blue]{page_url}[/blue]")

        except Exception as e:
            console.print(f"[red]Error parsing page {page_url}[/red]: {e}")

    async def parse_article(self, page_url, html):
        try:
            filtered_strings = [p for p in html.css("div#content-main p").getall()]
            filtered_content = hypertext.unescape(" <br/> ".join(filtered_strings))

            item = {
                "source": "anacao",
                "title": html.css("header#post-header h1::text").get(),
                "author": html.css("header#post-header span.author-name a::text").get(),
                "date_pub": normalize_date(
                    dateparser.parse(html.css("time.post-date::text").get())
                ),
                "link": page_url,
                "topic": html.css("header#post-header span::text").get(),
                "text_html": filtered_content,
            }

            # Put the item into the storage queue
            await self.storage_queue.put(item)

        except Exception as e:
            console.print(f"[red]Error parsing article {page_url}[/red]: {e}")


async def main():
    start_urls_anacao = [
        "/categoria/sociedade/",
        "/categoria/politica/",
        "/categoria/cultura/",
        "/categoria/economia/",
        "/categoria/desporto/",
        "/categoria/mundo/",
        "/categoria/diaspora/",
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
