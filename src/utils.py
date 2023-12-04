import asyncio
import os
from datetime import datetime

from dotenv import dotenv_values
from icecream import ic
from supabase import Client, create_client

dotenv_path = os.path.abspath("../.env")

config = dotenv_values(dotenv_path)


def normalize_date(date_obj):
    if not isinstance(date_obj, datetime):
        return None
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")


async def article_exist(link: str, source: str) -> bool:
    True


"""     url: str = config.get("SUPABASE_URL")
    key: str = config.get("SUPABASE_SECRET_KEY")
    supabase: Client = await create_client(url, key)

    data = await supabase.table("articles") \
        .select("*") \
        .eq("link", link) \
        .eq("source", source) \
        .execute()

    return True if len(data[0]) > 0 else False """
