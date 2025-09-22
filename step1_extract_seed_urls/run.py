import io
import csv
import random
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, Playwright, Error

import config
from storage_strategies import get_storage_strategy
from utils import convert_rows_to_in_memory_csv

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
APP_ENV = config.APP_ENV
LOCAL_STORAGE_DIR = config.LOCAL_STORAGE_DIR
GCS_BUCKET_NAME = config.GCS_BUCKET_NAME
TIMEOUT_MS = config.TIMEOUT * 1000
DEFAULT_FILENAME = config.STEP1_OUTPUT_FILENAME
USER_AGENTS = config.USER_AGENTS

# --- Logic Constants ---
TARGET_URL = "https://support.google.com/youtube#topic="
BASE_URL = "https://support.google.com"


def _fetch_urls(p: Playwright, target_url: str, base_url: str, timeout: int, user_agent: str) -> list[str]:
    """
    Navigates to a URL using Playwright and extracts a list of anchor hrefs.
    Converts relative URLs to absolute URLs.
    """
    extracted_urls = []

    logger.debug(f"Launching browser with user_agent: '{user_agent}'")
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(user_agent=user_agent)
    page = context.new_page()

    try:
        logger.info(f"Navigating to page: {target_url}")
        page.goto(target_url, timeout=timeout)

        container_selector = "article.article nav.accordion-homepage"
        logger.info(f"Waiting for container element: '{container_selector}'")
        page.wait_for_selector(container_selector, timeout=timeout)

        link_selector = f"{container_selector} a"
        logger.info(f"Querying for link elements: '{link_selector}'")
        link_elements = page.query_selector_all(link_selector)
        logger.debug(f"Found {len(link_elements)} link elements.")

        for link_element in link_elements:
            href = link_element.get_attribute("href")
            if href:
                full_url = urljoin(base_url, href)
                extracted_urls.append(full_url)

    finally:
        logger.info("Closing browser context.")
        browser.close()

    return extracted_urls


# execute関数 は TARGET_URL や、BASE_URL などのコンテキスト、つまりモジュールのグローバル定数を知っているという丁で関数を書く。
# しかし、それと同時に、これらを外部から一時的に変更できるような設計にもしたいので、デフォルト値としてこれら定数を注入するようにする。
def execute(target_url: str = TARGET_URL, base_url: str = BASE_URL):
    """
    The main execution function. It fetches URLs and saves them
    using a storage strategy determined by the environment.
    """
    logger.info("--- Step 1: Starting Seed URLs Extraction ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    # Get the storage strategy based on the current environment
    app_config = {
        'LOCAL_STORAGE_DIR': LOCAL_STORAGE_DIR,
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
    }
    storage_saver = get_storage_strategy(APP_ENV, app_config)
    logger.info(f"Using storage strategy: '{storage_saver.__class__.__name__}'")

    try:
        selected_user_agent = random.choice(USER_AGENTS)
        # Step 1: Fetch URLs using Playwright
        with sync_playwright() as p:
            urls = _fetch_urls(p, target_url, base_url, timeout=TIMEOUT_MS, user_agent=selected_user_agent)

        # Step 2: Process the extracted URLs
        if not urls:
            logger.warning("No URLs found on the page. Process will finish without creating an output file.")
            return

        logger.info(f"Successfully extracted a total of {len(urls)} URLs.")


        # Step 3: Write URLs to an in-memory virtual CSV
        logger.info("Preparing extracted URLs for CSV conversion...")
        # データ形式を List[str] から List[List[str]] に変換する
        rows_to_write = [[url] for url in urls]
        logger.info("Converting extracted URLs to in-memory CSV buffer...")
        string_io = convert_rows_to_in_memory_csv(rows_to_write)
        logger.info("In-memory CSV buffer created successfully.")

        # Step 4: Use the selected strategy to save the file
        logger.info(f"Attempting to save URLs to '{DEFAULT_FILENAME}'...")
        storage_saver.save(string_io, DEFAULT_FILENAME)
        logger.info(f"Successfully saved URLs to '{DEFAULT_FILENAME}'.")

    except Error as e:
        # This catches Playwright-specific errors
        logger.error(f"A Playwright-specific error occurred: {e}", exc_info=True)
    except Exception as e:
        # This catches any other unexpected errors
        logger.error(f"An unexpected error occurred during execution: {e}", exc_info=True)
        # In case of an error, we end with a failure message.
        logger.info("--- Step 1: Finished with errors ---")
        return # Explicitly exit after logging the error

    logger.info("--- Step 1: Finished successfully ---")



if __name__ == "__main__":
    setup_logging()
    execute()