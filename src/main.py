# main_script.py

import asyncio
from .AnacaoScraper import main as anacaoMain
from .SantiagoMagazineScraper import main as stMain
from .ExpressDasIlhasScraper import main as expMain

async def main():
    # Run script1_main and script2_main concurrently
    await asyncio.gather(anacaoMain(), stMain(), expMain())

if __name__ == "__main__":
    asyncio.run(main())
