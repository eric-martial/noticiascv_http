import asyncio

from .AnacaoScraper import main as AnacaoMain
from .ExpressDasIlhasScraper import main as ExpressoDasIlhasMain
from .SantiagoMagazineScraper import main as SantiagoMagazineMain


async def main():
    await asyncio.gather(AnacaoMain(), SantiagoMagazineMain(), ExpressoDasIlhasMain())


if __name__ == "__main__":
    asyncio.run(main())
