# step2_crawl_all_urls/run.py

import io
import csv
import time
import random
import logging
import logging.config
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, Page, Error, Locator

# --- Project-specific imports ---
import config
from config_logging import LOGGING_CONFIG
from storage_strategies import get_storage_strategy

# --- Logging Setup ---
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
APP_ENV = config.APP_ENV
GCS_BUCKET_NAME = config.GCS_BUCKET_NAME
DEFAULT_OUTPUT_DIR = config.DEFAULT_OUTPUT_DIR
TIMEOUT_MS = config.TIMEOUT * 1000
USER_AGENTS = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"


# --- Logic Constants ---
BASE_URL = "https://support.google.com"
MAX_RECURSION_DEPTH = 4  # 再帰の最大深度
YOUTUBE_ANSWER_STRING = 'youtube/answer'
YOUTUBE_TOPIC_STRING = 'youtube/topic'
STEP1_FILENAME = config.STEP1_OUTPUT_FILENAME
STEP2_FILENAME = "raw_urls_list.csv"  # このステップの出力ファイル名


class Crawler:
    """
    Manages the recursive crawling process using a single Playwright Page instance.
    """

    def __init__(self, page: Page):
        self.page = page
        self.results = []

    def _safe_get_text(self, locator: Locator) -> str:
        """Safely gets text from a locator, returning "" if it doesn't exist."""
        if locator.count() > 0:
            return locator.inner_text().strip()
        return ""

    def _modify_url(self, original_url: str) -> str:
        """
        Converts a relative URL to an absolute URL and sets the language to English.
        """
        if not original_url:
            return ""

        full_url = urljoin(BASE_URL, original_url)
        parsed_url = urlparse(full_url)

        # クエリパラメータを維持しつつ、hl=enを追加または上書きする
        # （元のコードのロジックをより堅牢にしました）
        query = parsed_url.query
        params = dict(p.split('=') for p in query.split('&')) if query else {}
        params['hl'] = 'en'
        new_query = '&'.join([f"{k}={v}" for k, v in params.items()])

        return parsed_url._replace(query=new_query).geturl()

    def scrape_page(self, url: str, title: str = "", depth: int = 0):
        """
        Recursively scrapes a page to find links, mimicking the original logic.
        """
        if depth > MAX_RECURSION_DEPTH:
            logger.warning(f"Max recursion depth ({MAX_RECURSION_DEPTH}) reached. Stopping crawl at: {url}")
            return

        logger.info(f"Scraping [Depth: {depth}]: {url}")

        try:
            time.sleep(random.uniform(1.5, 2.5)) # Be a polite crawler
            self.page.goto(url, timeout=TIMEOUT_MS, wait_until="domcontentloaded")

            # `topic-container` セクションを探す
            section = self.page.locator('section.topic-container')
            if section.count() == 0:
                 # 特殊ケース: topic-container がない answer ページのハンドリング
                logger.warning(f"No 'section.topic-container' found on page: {url}. Assuming it's a leaf page.")
                return

            h1_text = self._safe_get_text(section.locator('h1'))
            if not h1_text:
                logger.warning(f"No h1 title found on page: {url}")

            # `topic-children` (リンクのリスト部分) を探す
            topic_children = self.page.locator('div.topic-children')
            if topic_children.count() == 0:
                logger.warning(f"No 'div.topic-children' found on page: {url}. No further links to process.")
                return

            # 子要素のdivを全て取得
            child_divs = topic_children.locator('> div').all() # `>` は直接の子要素のみを対象
            if not child_divs:
                # divでラップされていない場合を考慮
                child_divs = [topic_children]

            for child_div in child_divs:
                mid_title = self._safe_get_text(child_div.locator('h2'))

                # タイトルの階層を構築
                if mid_title:
                    full_title = f"{title}__{h1_text}__{mid_title}" if title else f"{h1_text}__{mid_title}"
                else:
                    full_title = f"{title}__{h1_text}" if title else h1_text

                a_tags = child_div.locator('a[href]').all()
                if not a_tags:
                    logger.warning(f"No links found in category '{mid_title or 'main'}' on page: {url}")
                    continue

                for a_tag in a_tags:
                    href = a_tag.get_attribute('href')
                    modified_url = self._modify_url(href)

                    if not modified_url:
                        continue

                    if YOUTUBE_ANSWER_STRING in modified_url:
                        logger.debug(f"Found Answer URL: {modified_url}")
                        self.results.append({full_title: modified_url})
                    elif YOUTUBE_TOPIC_STRING in modified_url:
                        # 再帰呼び出し
                        self.scrape_page(modified_url, full_title, depth + 1)
                    else:
                        logger.warning(f"URL is out of scope (not answer/topic): {modified_url}")

        except Error as e:
            logger.error(f"A Playwright error occurred while scraping {url}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while scraping {url}: {e}")


def execute():
    """Main execution function for step 2."""
    logger.info("--- Step 2: Starting Recursive URL Crawling ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    storage = get_storage_strategy(APP_ENV, {
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
        'DEFAULT_OUTPUT_DIR': DEFAULT_OUTPUT_DIR,
    })
    logger.info(f"Using storage strategy: '{storage.__class__.__name__}'")

    try:
        # 1. Step1で作成したシードURLリストを読み込む
        logger.info(f"Loading seed URLs from '{STEP1_FILENAME}'...")
        # storage.read() は StringIO オブジェクトを直接返す
        string_io = storage.read(STEP1_FILENAME)
        # csv.reader は StringIO オブジェクトを直接読み込める
        reader = csv.reader(string_io)
        seed_urls = [row[0] for row in reader if row]
        if not seed_urls:
            logger.critical("No seed URLs found. Aborting.")
            return
        logger.info(f"Loaded {len(seed_urls)} seed URLs.")

        # 2. Playwrightを起動し、クロール処理を実行
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = context.new_page()

            crawler = Crawler(page)
            for url in seed_urls:
                logger.info(f"--- Starting crawl from top-level URL: {url} ---")
                crawler.scrape_page(url)
                logger.info(f"--- Finished crawl for top-level URL: {url} ---")

            browser.close()

        all_articles = crawler.results
        if not all_articles:
            logger.warning("Crawling finished, but no articles were collected.")
            return

        # 3. 結果をCSVに書き出す
        logger.info(f"Writing {len(all_articles)} discovered articles to in-memory CSV...")
        output_io = io.StringIO()
        writer = csv.writer(output_io)
        for article_dict in all_articles:
            for title, url in article_dict.items():
                writer.writerow([title, url])

        logger.info(f"Saving articles to '{STEP2_FILENAME}'...")
        storage.save(output_io, STEP2_FILENAME)
        logger.info(f"Successfully saved {len(all_articles)} articles.")

    except FileNotFoundError:
        logger.critical(f"Input file '{STEP1_FILENAME}' not found. Please run Step 1 first.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}", exc_info=True)
        logger.info("--- Step 2: Finished with errors ---")
        return

    logger.info("--- Step 2: Finished successfully ---")


if __name__ == "__main__":
    execute()