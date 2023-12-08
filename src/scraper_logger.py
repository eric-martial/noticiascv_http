import logging
import os

from rich.console import Console

log_folder = "runtime"
log_file = f"{log_folder}/scrapers.log"

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Set up a file handler with 'a' (append) mode
file_handler = logging.FileHandler(log_file, mode="a")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)


class ScraperLogger:
    console = Console()

    @staticmethod
    def log_info(message):
        logger.info(message)
        ScraperLogger.console.print(f"[green]INFO:[/green] {message}")

    @staticmethod
    def log_warning(message):
        logger.warning(message)
        ScraperLogger.console.print(f"[yellow]WARNING:[/yellow] {message}")

    @staticmethod
    def log_error(message):
        logger.error(message)
        ScraperLogger.console.print(f"[red]ERROR:[/red] {message}")
