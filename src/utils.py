from datetime import datetime
import os

from dotenv import dotenv_values

dotenv_path = os.path.abspath("../.env")

config = dotenv_values(dotenv_path)


def normalize_date(date_obj):
    if not isinstance(date_obj, datetime):
        return None
    return date_obj.strftime("%Y-%m-%d %H:%M:%S")
