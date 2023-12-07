# main_script.py

import asyncio
from .AnacaoScraper import main as AnacaoMain
from .SantiagoMagazineScraper import main as SantiagoMagazineMain
from .ExpressDasIlhasScraper import main as ExpressoDasIlhasMain

async def main():
    # Run script1_main and script2_main concurrently
    await asyncio.gather(AnacaoMain(), SantiagoMagazineMain(), ExpressoDasIlhasMain())

if __name__ == "__main__":
    asyncio.run(main())
