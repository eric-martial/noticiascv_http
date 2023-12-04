from queue import Empty

import aiosqlite
from rich.console import Console

SENTINEL = "STOP"

console = Console()


class StorageWorker:
    def __init__(self, storage_queue, database_path):
        self.storage_queue = storage_queue
        self.database_path = database_path

    async def run(self):
        # Initialize SQLite database and table
        await self.init_database()

        while True:
            try:
                item = await self.storage_queue.get()
                if item == SENTINEL:
                    break

                if not await self.article_exists(item["link"]):
                    await self.save_to_storage(item)
                else:
                    console.print(f"[yellow]Skipped existing article:[/yellow] {item}")
            except Empty:
                pass

    async def article_exists(self, link):
        async with aiosqlite.connect(self.database_path) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                "SELECT COUNT(*) FROM articles WHERE link = ?", (link,)
            )
            count = await cursor.fetchone()
            return count[0] > 0

    async def init_database(self):
        async with aiosqlite.connect(self.database_path) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT,
                    title TEXT,
                    author TEXT,
                    date_pub TEXT,
                    link TEXT,
                    topic TEXT,
                    text_html TEXT
                )
            """
            )
            await conn.commit()

    async def save_to_storage(self, item):
        async with aiosqlite.connect(self.database_path) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                """
                INSERT INTO articles
                (source, title, author, date_pub, link, topic, text_html)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    item["source"],
                    item["title"],
                    item["author"],
                    item["date_pub"],
                    item["link"],
                    item["topic"],
                    item["text_html"],
                ),
            )
            await conn.commit()

        console.print(f"[green]Saved item to SQLite database:[/green] {item}")
