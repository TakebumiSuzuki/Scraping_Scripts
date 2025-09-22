import io
import csv
import time
import random
import logging
from playwright.sync_api import sync_playwright, Error, Playwright, Browser

# --- Project-specific imports ---
# 親ディレクトリをsys.pathに追加して、プロジェクトのルートモジュールをインポート可能にする
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import config
from storage_strategies import get_storage_strategy, StorageStrategy, StorageFileNotFoundError
from config_logging import setup_logging

# --- Logger Setup ---
# mainブロックでsetup_logging()を呼び出すことで、設定が適用される
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
APP_ENV = config.APP_ENV
LOCAL_STORAGE_DIR = config.LOCAL_STORAGE_DIR
GCS_BUCKET_NAME = config.GCS_BUCKET_NAME
TIMEOUT_MS = config.TIMEOUT * 1000
USER_AGENTS = config.USER_AGENTS
STEP3_FILENAME = config.STEP3_OUTPUT_FILENAME


class Scraper:
    """
    Playwrightのライフサイクルを管理し、単一ページのHTMLコンテンツ取得に特化したクラス。
    step2のCrawlerの設計思想を継承。
    """
    def __init__(self, timeout_ms: int, user_agents: list[str]):
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.timeout_ms = timeout_ms
        self.user_agents = user_agents
        logger.info("Scraper class initialized.")

    def __enter__(self):
        """'with'構文でインスタンスが作成される際に呼び出される"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        logger.info("Playwright started and Chromium browser launched.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """'with'ブロックを抜ける際に、エラーの有無に関わらず呼び出される"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("Browser closed and Playwright stopped.")

    def scrape_html_content(self, url: str) -> str | None:
        """
        指定されたURLからHTMLコンテンツをスクレイピングする。
        過去のプロジェクトのロジック（クリックによる展開）を継承。
        """
        # 毎回新しいコンテキストとページを作成することで、Cookieやキャッシュの干渉を防ぐ
        context = self.browser.new_context(user_agent=random.choice(self.user_agents))
        page = context.new_page()
        logger.info(f"Scraping page: {url}")

        try:
            page.goto(url, timeout=self.timeout_ms, wait_until="networkidle")

            # 過去のコードにあった、クリックしてコンテンツを展開するロジック
            logger.info("Expanding page content by clicking zippy containers...")
            clickable_elements = page.locator("div.zippy-container > h2, div.zippy-container > a, div.zippy-container > h3").all()
            for i, element in enumerate(clickable_elements):
                try:
                    element.click(timeout=5000)
                    logger.debug(f"Clicked element {i+1}/{len(clickable_elements)}.")
                    time.sleep(random.uniform(0.3, 0.7)) # 念のため短い待機
                except Error as e:
                    # 要素が消えたり、クリックできない状態だったりする場合があるため、警告に留める
                    logger.warning(f"Could not click an expandable element on {url}: {e}")

            # .article-container要素からHTMLを取得
            article_container = page.locator(".article-container")
            if article_container.count() == 0:
                logger.error(f"Target element '.article-container' not found on {url}.")
                return None

            html_content = article_container.inner_html()
            logger.info(f"Successfully extracted HTML content from {url}.")
            return html_content

        except Error as e:
            logger.error(f"A Playwright error occurred while scraping {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred while scraping {url}: {e}", exc_info=True)
            return None
        finally:
            context.close() # ページとコンテキストを閉じる
            logger.debug(f"Page and context for {url} closed.")


def execute() -> None:
    """Main execution function for step 4."""
    logger.info("--- Step 4: Starting Scrape and Save HTML ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    config_dict = {
        'LOCAL_STORAGE_DIR': LOCAL_STORAGE_DIR,
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
    }
    # step3までのCSVファイルを読み込むためのストレージ
    input_storage = get_storage_strategy(APP_ENV, config_dict)
    # step4でHTMLを保存するためのストレージ（dev環境ではSQLiteになる）
    output_storage = get_storage_strategy(APP_ENV, config_dict, step_context='step4')

    logger.info(f"Input Storage: '{input_storage.__class__.__name__}'")
    logger.info(f"Output Storage: '{output_storage.__class__.__name__}'")

    try:
        # 1. URLリストの読み込み
        logger.info(f"Loading unique URLs from '{STEP3_FILENAME}'...")
        string_io = input_storage.read(STEP3_FILENAME)
        # ヘッダー行をスキップする場合: next(reader, None)
        reader = csv.reader(string_io)
        urls_to_scrape = [row[0] for row in reader if row]
        if not urls_to_scrape:
            logger.critical("No URLs found in the input file. Aborting.")
            return
        logger.info(f"Loaded {len(urls_to_scrape)} URLs to scrape.")

        # 2. スクレイピングと保存のループ
        with Scraper(timeout_ms=TIMEOUT_MS, user_agents=USER_AGENTS) as scraper:
            total_urls = len(urls_to_scrape)
            for i, url in enumerate(urls_to_scrape):
                logger.info(f"--- Processing URL {i+1}/{total_urls}: {url} ---")

                # 再開機能：既に保存されているURLはスキップ
                if output_storage.exists(url):
                    logger.info(f"URL already exists in storage. Skipping.")
                    continue

                html_content = scraper.scrape_html_content(url)

                if html_content:
                    # 取得したHTMLをインメモリのStringIOに変換して保存
                    html_io = io.StringIO(html_content)
                    output_storage.save(html_io, filename=url)
                else:
                    logger.error(f"Failed to retrieve HTML for {url}. It will not be saved.")

                # 丁寧なスクレイピングのための待機
                sleep_time = random.uniform(2, 5)
                logger.info(f"Waiting for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

    except StorageFileNotFoundError:
        logger.critical(f"Input file '{STEP3_FILENAME}' not found. Please run up to Step 3 first.")
    except Exception as e:
        logger.critical(f"A critical error occurred during execution: {e}", exc_info=True)
        logger.info("--- Step 4: Finished with errors ---")
        return

    logger.info("--- Step 4: Finished successfully ---")


if __name__ == "__main__":
    # プロジェクト共通のロギング設定を適用
    setup_logging()
    execute()