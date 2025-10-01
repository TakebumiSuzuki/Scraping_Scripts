import io
import csv
import time
import random
import urllib.parse
from playwright.sync_api import sync_playwright, Error, Playwright, Browser, expect

import config
from storage_strategies import get_storage_strategy, StorageFileNotFoundError

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
APP_ENV = config.APP_ENV
TIMEOUT_MS = config.TIMEOUT * 1000
USER_AGENTS = config.USER_AGENTS
STEP3_FILENAME = config.STEP3_OUTPUT_FILENAME

class RedirectedURLSkipException(Exception):
    """リダイレクトが検出され、URLの処理をスキップすることを示すための例外。"""
    pass

class Scraper:
    """
    Playwrightのライフサイクルを管理し、単一ページのHTMLコンテンツ取得に特化したクラス。
    """
    def __init__(self, timeout_ms: int, user_agents: list[str]):
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.timeout_ms = timeout_ms
        self.user_agents = user_agents
        logger.info("Scraper class initialized.")

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        logger.info("Playwright started and Chromium browser launched.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("Browser closed and Playwright stopped.")

    def scrape_html_content(self, url: str) -> str | None:
        context = self.browser.new_context(user_agent=random.choice(self.user_agents))
        page = context.new_page()
        logger.info(f"Scraping page: {url}")
        try:
            # もし url からのレスポンスががリダイレクトを要求した場合には、リダイレクト先にアクセスした上で、
            # ブラウザで domcontentloaded シグナルが発火されるのを動機的に待つ。
            page.goto(url, timeout=self.timeout_ms, wait_until="domcontentloaded")

            logger.info("Expanding page content by clicking zippy containers...")

            # 1. locatorオブジェクトを先に定義する
            clickable_elements_locator = page.locator("div.zippy-container > h2, div.zippy-container > a, div.zippy-container > h3")

            # 最初の要素が表示されるまで少し待つ（任意だが堅牢性が増す）
            # これにより、コンテナ自体が存在しないページで無駄なループが走るのを防ぐ
            try:
                clickable_elements_locator.first.wait_for(timeout=3000)
            except Error:
                logger.warning(f"No expandable elements found on {url}. Proceeding without clicking.")
                # このページにクリック対象がない場合は、そのまま後続処理へ
                pass

            count = clickable_elements_locator.count()
            logger.info(f"Found {count} expandable elements.")


            # ページ読み込み直後、待機せずにリダイレクトされたかを判定する。
            # ヘルプトップページに特徴的な<h1 class="promoted-search__greeting">の存在をチェック。
            redirect_indicator = page.locator("h1.promoted-search__greeting")

            # .is_visible() はタイムアウトで待機せず、その瞬間の可視状態を即座に返す。
            if redirect_indicator.is_visible():
                logger.warning(f"URL was redirected to a main page. Skipping scrape for: {url}")
                raise RedirectedURLSkipException() # 例外を発生させる

            # 2. 次に、私のスクリプト環境で発生した「コンテンツ削除済み」ページをチェックする
            no_content_indicator = page.locator("div.no-content-message")
            if no_content_indicator.is_visible():
                logger.warning(f"Content is no longer available. Skipping: {url}")
                raise RedirectedURLSkipException()


            # 2. count()で取得した要素数でループし、nth(i)で各要素にアクセス
            for i in range(count):
                element = clickable_elements_locator.nth(i)
                try:
                    # 1. まず要素が「見える」状態になるまで待つ
                    expect(element).to_be_visible(timeout=3000)

                    # 2. 次に要素が「有効化」されている状態になるまで待つ
                    expect(element).to_be_enabled(timeout=3000)

                    element.click()
                    logger.debug(f"Clicked element {i+1}/{count}.")
                    time.sleep(random.uniform(0.3, 0.7))

                except Error as e:
                    logger.warning(f"Could not click an expandable element #{i+1} on {url}: {e}")
            article_container = page.locator(".article-container")
            try:
                # .article-containerが画面に表示されるまで最大3秒待つ
                expect(article_container).to_be_visible(timeout=3000)
            except Error:
                # タイムアウトした場合
                logger.error(f"Target element '.article-container' not found or not visible on {url}.")
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
            context.close()
            logger.debug(f"Page and context for {url} closed.")

def execute(interaction_dir) -> None:
    """Main execution function for step 4."""
    logger.info("--- Step 4: Starting Scrape and Save HTML ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    input_storage = get_storage_strategy(APP_ENV, interaction_dir)
    output_storage = get_storage_strategy(APP_ENV, interaction_dir, step_context='step4')

    logger.info(f"Input Storage: '{input_storage.__class__.__name__}'")
    logger.info(f"Output Storage: '{output_storage.__class__.__name__}'")

    try:
        # 1. URLリストとカテゴリの読み込み
        logger.info(f"Loading unique URLs from '{STEP3_FILENAME}'...")
        string_io = input_storage.read(STEP3_FILENAME)
        reader = csv.reader(string_io)
        urls_to_process = []
        for row in reader:
            if row and len(row) > 1:
                urls_to_process.append((row[0], row[1])) # (category, url)
            else:
                logger.warning(f"Skipping malformed row in CSV: {row}")

        if not urls_to_process:
            logger.critical("No valid URLs found. Aborting.")
            return
        logger.info(f"Loaded {len(urls_to_process)} URLs with categories.")

        # 2. スクレイピングと保存のループ (リトライ機構付き)
        MAX_ATTEMPTS = 3  # 最初の試行 + 2回のリトライ

        with Scraper(timeout_ms=TIMEOUT_MS, user_agents=USER_AGENTS) as scraper:
            # このループが試行回数を制御する
            for attempt in range(1, MAX_ATTEMPTS + 1):
                # 処理すべきURLがなければループを抜ける
                if not urls_to_process:
                    logger.info("No more URLs to process. All tasks completed successfully.")
                    break

                logger.info(f"--- [ATTEMPT {attempt}/{MAX_ATTEMPTS}] Processing {len(urls_to_process)} URLs... ---")

                failures_in_this_attempt = []
                total_in_pass = len(urls_to_process) # これは単純に logger での表記用

                for i, (category, url) in enumerate(urls_to_process):
                    logger.info(f"--- Processing URL {i+1}/{total_in_pass}: {url} ---")

                    # 最初の試行でのみ、既存チェックを行う。これは、再開可能性（Resumability）」の担保のため。
#                   # 長時間かかるバッチ処理を設計する際のベストプラクティスの一つ。
                    if attempt == 1 and output_storage.exists(url):
                        logger.info(f"URL already exists. Skipping.")
                        continue

                    try:
                        html_content = scraper.scrape_html_content(url)
                        if not html_content:
                            raise ValueError("Scraping returned None, indicating a failure.")

                        html_io = io.StringIO(html_content)
                        metadata = {'category': category}
                        # スラッシュはGCSで回想とみなされてしまうので、それを + という文字に変換する
                        safe_filename = urllib.parse.quote_plus(url)
                        output_storage.save(html_io, filename=safe_filename, metadata=metadata)

                    except RedirectedURLSkipException:
                        # リダイレクトによるスキップは「失敗」ではないので、ログにも残さず、
                        # リトライリストにも追加しない。ただ静かに次のURLへ進む。
                        pass

                    except Exception as e:
                        logger.error(f"Failed on attempt {attempt} for URL {url}: {e}")
                        failures_in_this_attempt.append((category, url))

                    # 待機時間。試行回数が増えるごとに少し長く待つようにする
                    sleep_time = random.uniform(1.5, 3) * attempt
                    logger.info(f"Waiting for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)

                # 次のループのために、処理対象を今回の失敗リストに更新する
                urls_to_process = failures_in_this_attempt

        # 3. 最終的な結果の報告
        # ループがすべて終わった後に `urls_to_process` に残っているものが、最終的に失敗したURL
        if urls_to_process:
            permanently_failed_urls = urls_to_process
            logger.critical(f"--- The following {len(permanently_failed_urls)} URLs could not be scraped after {MAX_ATTEMPTS} attempts ---")
            for category_fail, url_fail in permanently_failed_urls:
                logger.critical(f"Category: {category_fail}, URL: {url_fail}")
        else:
            logger.info("All URLs were processed successfully within the retry limits.")

    except StorageFileNotFoundError:
        logger.critical(f"Input file '{STEP3_FILENAME}' not found. Please run up to Step 3 first.")
    except Exception as e:
        logger.critical(f"A critical unrecoverable error occurred: {e}", exc_info=True)
        logger.info("--- Step 4: Finished with errors ---")
        return

    logger.info("--- Step 4: Finished successfully ---")


if __name__ == "__main__":
    setup_logging()
    execute(interaction_dir='outputs/test')