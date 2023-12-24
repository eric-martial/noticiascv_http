import asyncio

from .anacao_scraper import main as anacao_main
from .expresso_das_ilhas_scraper import main as expressodasilhas_main
from .santiagomagazine_scraper import main as santiagomagazine_main


async def main():
    await asyncio.gather(anacao_main(), santiagomagazine_main())


if __name__ == "__main__":
    asyncio.run(main())
