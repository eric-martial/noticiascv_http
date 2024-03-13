import asyncio

from .scrapers.anacao_scraper import main as anacao_main
from .scrapers.expressodasilhas_scraper import main as expressodasilhas_main
from .scrapers.santiagomagazine_scraper import main as santiagomagazine_main


async def main():
    await asyncio.gather(
        anacao_main(), expressodasilhas_main(), santiagomagazine_main()
    )


if __name__ == "__main__":
    asyncio.run(main())
