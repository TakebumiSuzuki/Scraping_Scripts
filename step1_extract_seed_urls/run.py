import os
import sys
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
OUTPUT_BASE_DIR = config.OUTPUT_BASE_DIR
TIMEOUT_MS = config.TIMEOUT * 1000
STEP1_OUTPUT_FILENAME = config.STEP1_OUTPUT_FILENAME
USER_AGENTS = config.USER_AGENTS

# --- Logic Constants ---
ENTRY_URL = "https://support.google.com/youtube#topic="
BASE_URL = "https://support.google.com"
WAIT_FOR_SELECTOR = "article.article nav.accordion-homepage section:last-child a"
QUERY_SELECTOR = "article.article nav.accordion-homepage section a"


def _fetch_urls(
    p: Playwright,
    entry_url: str,
    base_url: str,
    timeout: int,
    user_agent: str,
    wait_for_selector: str,
    query_selector: str
) -> list[str]:
    """
    Navigates to a URL using Playwright and extracts a list of anchor hrefs.
    Converts relative URLs to absolute URLs.
    """
    extracted_absolute_urls = []

    logger.debug(f"Launching browser with user_agent: '{user_agent}'")
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(user_agent=user_agent)
    page = context.new_page()

    try:
        logger.info(f"Navigating to page: {entry_url}")
        page.goto(entry_url, timeout=timeout)

        # logger.info(f"Waiting for the last container's target element: '{wait_for_selector}'")

        # page.wait_for_selector() は、指定されたCSSセレクタに一致する要素がページのDOM内に少なくとも1つ
        # 出現した時点で、待機を完了し、すぐに次のコードの実行に移ります。
        # 本来、非同期版のPlaywrightでは await page.wait_for_selector(...) と書く必要があるが、
        # このコードで使われている同期版（sync_playwright）では、awaitなしで実行できるようになっている。
        # page.wait_for_selector(container_selector, timeout=timeout)

        # logger.info(f"Waiting for at least one link element to appear: '{link_selector}'")
        # "article.article nav.accordion-homepage a" というセレクタに一致する
        # 最初の <a> 要素が現れるまで待機する。
        # page.wait_for_selector(link_selector, timeout=timeout)


        logger.info(f"Waiting for the last target element to be attached to the DOM: '{wait_for_selector}'")

        # page.wait_for_selector() は、指定されたCSSセレクタに一致する要素がページのDOM内に少なくとも1つ
        # 表示された時点で、待機を完了し、すぐに次のコードの実行に移ります。
        # 本来、非同期版のPlaywrightでは await page.wait_for_selector(...) と書く必要があるが、
        # このコードで使われている同期版（sync_playwright）では、awaitなしで実行できるようになっている。
        # ここでは、state='attached'により、最後のリンクが「表示」されるまでではなく、「DOMにアタッチ」されるまで待つ
        page.wait_for_selector(wait_for_selector, timeout=timeout, state='attached')

        # この時点で、ほぼ全てのリンクがDOMに存在することが期待できる
        logger.info(f"Querying for link elements: '{query_selector}'")
        link_elements = page.query_selector_all(query_selector)
        logger.debug(f"Found {len(link_elements)} link elements.")

        for link_element in link_elements:
            href = link_element.get_attribute("href")
            if href:
                full_url = urljoin(base_url, href)
                extracted_absolute_urls.append(full_url)

    finally:
        logger.info("Closing browser context.")
        # browser.close() を呼び出すと、Browser インスタンスに属するすべての BrowserContext と、それに付随するすべての
        # Page が自動的に、かつ適切に閉じられ、関連するリソース（メモリやプロセスなど）がすべて解放されます。
        # よってここで page.close() などを追加で呼び出す必要はありません。
        browser.close()

    return extracted_absolute_urls


# execute関数 は ENTRY_URL や、BASE_URL などのコンテキスト、つまりモジュールのグローバル定数を知っているという丁で関数を書く。
# しかし、それと同時に、これらを外部から一時的に変更できるような設計にもしたいので、デフォルト値としてこれら定数を注入するようにする。
def execute(
    interaction_dir,
    entry_url: str = ENTRY_URL,
    base_url: str = BASE_URL,
    wait_for_selector: str = WAIT_FOR_SELECTOR,
    query_selector: str = QUERY_SELECTOR
) -> None:
    """
    Extracts all link URLs from a target web page and saves them to a CSV file.

    This function uses a dynamically selected storage strategy (e.g., local file
    or cloud storage) based on the application environment to persist the URLs.
    """
    logger.info("--- Step 1: Starting Seed URLs Extraction ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    storage_saver = get_storage_strategy(APP_ENV, interaction_dir)
    logger.info(f"Using storage strategy: '{storage_saver.__class__.__name__}'")

    try:
        # Step 1: Fetch URLs using Playwright
        with sync_playwright() as p:
            urls = _fetch_urls(
                p,
                entry_url,
                base_url,
                timeout=TIMEOUT_MS,
                user_agent=random.choice(USER_AGENTS),
                wait_for_selector=wait_for_selector,
                query_selector=query_selector
            )

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
        logger.info(f"Attempting to save URLs to '{STEP1_OUTPUT_FILENAME}' in directory '{interaction_dir}'...")
        storage_saver.save(string_io, STEP1_OUTPUT_FILENAME)
        logger.info(f"Successfully saved URLs to '{STEP1_OUTPUT_FILENAME}' in directory '{interaction_dir}'.")

    except Exception as e: # まず全てのエラーをここで捕捉する
        if isinstance(e, Error): # Playwright固有のエラーの場合
            logger.error(f"A Playwright-specific error occurred: {e}", exc_info=True)
        else: # その他の予期せぬエラーの場合
            logger.error(f"An unexpected error occurred during execution: {e}", exc_info=True)

        logger.info("--- Step 1: Finished with errors ---")
        return

    logger.info("--- Step 1: Finished successfully ---")



if __name__ == "__main__":
    # コマンドライン引数が存在すれば、それで上書きする
    if len(sys.argv) > 1:
        run_id_arg = sys.argv[1]
        interaction_dir = os.path.join(OUTPUT_BASE_DIR, run_id_arg)
    else:
        interaction_dir = os.path.join(OUTPUT_BASE_DIR, 'test')

    setup_logging(base_dir=interaction_dir)
    execute(interaction_dir)