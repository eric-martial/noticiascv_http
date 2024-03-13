# noticiascv_http
Web scraper for Cape-Verdean news website using httpx package and asyncio. It uses queue to manage scraped items before saving them to db.
Websites currently scraped are:
- [Anacao](https://www.anacao.cv/)
- [Santiagmagazine](https://santiagomagazine.cv/)
- [ExpressoDasIlhas](https://expressodasilhas.cv/)

Run by doing ```python -m src.main```