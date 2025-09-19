import io
import csv
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, Playwright, Error
import config
import logging.config
from storage_strategies import get_storage_strategy # Import the factory

import logging
from config_logging import LOGGING_CONFIG

# --- Logging Setup ---
logging.config.dictConfig(LOGGING_CONFIG)
# 実際には logging.getLogger('step1_extract_seed_urls.run') と書いているのと同じ意味
# フォーマッタには %(name)s という項目があります。これにより、ログファイルには
# - step1_extract_seed_urls.run - INFO - 処理を開始しました のように、
# どのモジュールのどのロガーが出力したログなのかが自動的に記録されます。
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
APP_ENV = config.APP_ENV
GCS_BUCKET_NAME = config.GCS_BUCKET_NAME
DEFAULT_OUTPUT_DIR = config.DEFAULT_OUTPUT_DIR
TIMEOUT_MS = config.TIMEOUT * 1000
DEFAULT_FILENAME = config.STEP1_OUTPUT_FILENAME

# --- Logic Constants ---
TARGET_URL = "https://support.google.com/youtube#topic="
BASE_URL = "https://support.google.com"


def _fetch_urls(p: Playwright, target_url: str, base_url: str, timeout: int) -> list[str]:
    """
    Navigates to a URL using Playwright and extracts a list of anchor hrefs.
    Converts relative URLs to absolute URLs.
    """
    extracted_urls = []
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

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

# execute はコンテキストを知っていますが、そのコンテキストを外部から一時的に変更する「裏口」を用意しておく
def execute(target_url: str = TARGET_URL, base_url: str = BASE_URL):
    """
    The main execution function. It fetches URLs and saves them
    using a storage strategy determined by the environment.
    """
    logger.info("--- Step 1: Starting Seed URL Extraction ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    # Get the storage strategy based on the current environment
    app_config = {
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
        'DEFAULT_OUTPUT_DIR': DEFAULT_OUTPUT_DIR,
    }
    storage_saver = get_storage_strategy(APP_ENV, app_config)
    logger.info(f"Using storage strategy: '{storage_saver.__class__.__name__}'")

    try:
        # Step 1: Fetch URLs using Playwright
        with sync_playwright() as p:
            urls = _fetch_urls(p, target_url, base_url, timeout=TIMEOUT_MS)

        # Step 2: Process the extracted URLs
        if not urls:
            logger.warning("No URLs found on the page. Process will finish without creating an output file.")
            return

        logger.info(f"Successfully extracted a total of {len(urls)} URLs.")

        # Step 3: Write URLs to an in-memory virtual CSV
        logger.info("Writing extracted URLs to in-memory CSV buffer.")
        string_io = io.StringIO()
        writer = csv.writer(string_io)
        for url in urls:
            # csv.writer.writerow() expects an iterable (like a list) for each row.
            writer.writerow([url])
        logger.debug("In-memory CSV buffer created successfully.")

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
    execute()