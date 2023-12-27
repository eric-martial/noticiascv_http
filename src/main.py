import asyncio

from .crawlers.anacao_scraper import main as anacao_main
from .crawlers.expressodasilhas_scraper import main as expressodasilhas_main
from .crawlers.santiagomagazine_scraper import main as santiagomagazine_main


async def main():
    await asyncio.gather(expressodasilhas_main(), expressodasilhas_main())


if __name__ == "__main__":
    asyncio.run(main())
