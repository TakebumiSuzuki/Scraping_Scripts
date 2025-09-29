import csv
import time
import random
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, Error, Locator, BrowserContext, Playwright, Browser

import config
from storage_strategies import get_storage_strategy
from utils import convert_rows_to_in_memory_csv

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)


APP_ENV = config.APP_ENV
TIMEOUT_MS = config.TIMEOUT * 1000
USER_AGENTS = config.USER_AGENTS
STEP1_OUTPUT_FILENAME = config.STEP1_OUTPUT_FILENAME
STEP2_OUTPUT_FILENAME = config.STEP2_OUTPUT_FILENAME

class Crawler:
    """
    A self-contained component that manages the Playwright lifecycle
    for web scraping, including browser setup and teardown.
    """
    MAX_RECURSION_DEPTH = 4
    YOUTUBE_ANSWER_STRING = 'youtube/answer'
    YOUTUBE_TOPIC_STRING = 'youtube/topic'
    BASE_URL = "https://support.google.com"

    def __init__(self, timeout_ms: int, user_agents: list[str]):
        self.playwright: Playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.results = []
        self.timeout_ms = timeout_ms
        self.user_agents = user_agents

    def __enter__(self):
        """Executed when entering the 'with' statement."""
        # start()はバックグラウンドでPlaywrightのサーバープロセスが起動し、接続準備が整うまで処理を同期処理、つまりブロックします。
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context()
        logger.info("Crawler initialized: Playwright started, Browser launched.")
        # with ... as crawler: の crawler にこのインスタンス自身を返す
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Executed upon exiting the 'with' block, ensuring resources are cleaned up."""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        logger.info("Crawler cleaned up: Context, Browser, and Playwright stopped.")

    def _safe_get_text(self, locator: Locator) -> str:
        """Safely gets text from a locator, returning "" if it doesn't exist."""
        if locator.count() > 0:
            # inner_text()は最初に見つかった要素のテキストだけを返します。
            return locator.inner_text().strip()
        return ""

    def _build_absolute_url_with_en(self, original_url: str) -> str:
        """
        Converts a relative URL to an absolute URL and sets the language to English.
        """
        if not original_url:
            return ""

        full_url = urljoin(self.BASE_URL, original_url)
        parsed_url = urlparse(full_url)
        query = parsed_url.query
        params_dict = dict(p.split('=') for p in query.split('&')) if query else {}
        params_dict['hl'] = 'en'
        new_query = '&'.join([f"{k}={v}" for k, v in params_dict.items()])
        return parsed_url._replace(query=new_query).geturl()

    def scrape_page(self, url: str, parent_title: str = "", depth: int = 0) -> None:
        """
        Scrapes a single URL. This method is self-contained,
        handling the entire lifecycle of a Page object from creation to disposal.
        """
        if depth > self.MAX_RECURSION_DEPTH:
            logger.debug(
            f"Max recursion depth ({self.MAX_RECURSION_DEPTH}) reached. Stopping crawl for this branch at URL: {url}")
            return

        page = self.context.new_page()
        page.set_extra_http_headers({'User-Agent': random.choice(self.user_agents)})
        logger.info(f"Scraping [Depth: {depth}]: {url}")

        try:
            page.goto(url, timeout=self.timeout_ms, wait_until="networkidle")
            time.sleep(random.uniform(1, 2))

            # locatorオブジェクト自体は「指示書」や「リモコン」のようなものだが、.count()や.all()のような特定のメソッドを
            # 呼び出してしまうと、その瞬間にPlaywrightはブラウザに問い合わせを行い、実際に要素の検索を実行する。
            # sectionsは Locatorオブジェクト。上記の通り、遅延実行の性質を持っている。
            sections = page.locator('section.topic-container')
            if sections.count() == 0:
                logger.warning(f"No 'section.topic-container' found on page: {url}. Assuming it's a leaf page.")
                return

            # _safe_get_textは、複数あったとしても、最初の h1 のみを取得する。
            h1_text = self._safe_get_text(sections.locator('h1'))
            if not h1_text:
                logger.warning(f"No h1 title found on page: {url}")

            topic_children = page.locator('div.topic-children')
            try:
                # 同期的に実行される。(非同期版の async_api を使用する場合には、await キーワードを使う必要がある。)
                topic_children.wait_for(state='visible', timeout=15000)
                logger.debug("'div.topic-children' container is visible and ready.")
            except Error:
                logger.warning(f"Content in 'div.topic-children' did not appear correctly on: {url}.")
                return

            # child_divsというのが、h2とそのトピックリストがまとまった島の集合
            child_divs = topic_children.locator('> div').all()
            if not child_divs:
                child_divs = [topic_children]

            # child_divは、h2とそのトピックリストがまとまった一つの島
            for child_div in child_divs:
                mid_title = self._safe_get_text(child_div.locator('h2'))

                title_parts = []
                if parent_title:
                    title_parts.append(parent_title)
                if h1_text:
                    title_parts.append(h1_text)
                if mid_title:
                    title_parts.append(mid_title)
                new_title = "__".join(title_parts)

                # a_tagsの取得は一度に行い、ループ内で属性を取得する
                a_tags = child_div.locator('a[href]').all()
                if not a_tags:
                    logger.warning(f"No links found in category '{mid_title or 'main'}' on page: {url}")
                    continue

                for a_tag in a_tags:
                    # 要素が存在しなくなっている可能性に対処するため、個別にtry-exceptで囲む
                    try:
                        href = a_tag.get_attribute('href', timeout=10000)
                        if not href:
                            continue
                    except Error as e:
                        logger.warning(f"Could not get href from an element in '{new_title}'. It might have been detached. Error: {e}")
                        continue

                    modified_url = self._build_absolute_url_with_en(href)
                    if not modified_url:
                        continue

                    if self.YOUTUBE_ANSWER_STRING in modified_url:
                        logger.debug(f"Found Answer URL: {modified_url}")
                        self.results.append({new_title: modified_url})
                    elif self.YOUTUBE_TOPIC_STRING in modified_url:
                        self.scrape_page(modified_url, new_title, depth + 1)
                    else:
                        logger.warning(f"URL is out of scope (not answer/topic): {modified_url}")

        except Error as e:
            logger.error(f"A Playwright error occurred on {page.url}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred on {page.url}: {e}")
        finally:
            page.close()


def execute(interaction_dir) -> None:
    """Main execution function for step 2."""
    logger.info("--- Step 2: Starting Recursive URL Crawling ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    storage = get_storage_strategy(APP_ENV, interaction_dir)
    logger.info(f"Using storage strategy: '{storage.__class__.__name__}'")

    try:
        logger.info(f"Loading seed URLs from '{STEP1_OUTPUT_FILENAME}'...")
        string_io = storage.read(STEP1_OUTPUT_FILENAME)
        reader = csv.reader(string_io)
        seed_urls = [row[0] for row in reader if row]
        if not seed_urls:
            logger.critical("No seed URLs found. Aborting.")
            return
        logger.info(f"Loaded {len(seed_urls)} seed URLs.")

        # manager = sync_playwright()としてしまうと、playwrightマネージャーができるだけ。__enter__()を呼ぶ必要がある。
        # __enter__()により、実質的には start()が呼ばれる。これはこのプロセスとは別のプロセスに Node.js サーバーを
        #　立ち上げ、このサーバーがブラウザに指示を出す。このプロセスとは　WebSocketにより接続される。
        # このプロセス上の`page`や`browser`などのオブジェクトは proxyオブジェクトであり、リモコンのようなもの。
        # page や browser からの指令がWebSocketを介してNode.jsサーバーに届き、その結果ブラウザが操作される。
        # 結局のところ、p は、Playwrightの全機能への入口となるオブジェクトで、Playwrightの実行環境
        # （バックグラウンドで動くNode.jsサーバープロセス）を安全に起動・終了するためのもの。

        with Crawler(timeout_ms=TIMEOUT_MS, user_agents=USER_AGENTS) as crawler:
            for url in seed_urls:
                crawler.scrape_page(url)

        all_articles = crawler.results
        if not all_articles:
            logger.warning("Crawling finished, but no articles were collected.")
            return

        logger.info(f"Preparing {len(all_articles)} discovered articles for CSV conversion...")
        # データ形式を List[Dict] から List[List[str]] に変換する
        rows_to_write = [[title, url] for article in all_articles for title, url in article.items()]

        logger.info("Converting articles to in-memory CSV buffer...")
        output_io = convert_rows_to_in_memory_csv(rows_to_write)
        logger.debug("In-memory CSV buffer created successfully.")

        logger.info(f"Saving articles to '{STEP2_OUTPUT_FILENAME}'...")
        storage.save(output_io, STEP2_OUTPUT_FILENAME)
        logger.info(f"Successfully saved {len(all_articles)} articles.")

    except FileNotFoundError:
        logger.critical(f"Input file '{STEP1_OUTPUT_FILENAME}' not found. Please run Step 1 first.")
        return
    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}", exc_info=True)
        logger.info("--- Step 2: Finished with errors ---")
        return

    logger.info("--- Step 2: Finished successfully ---")


if __name__ == "__main__":
    setup_logging()
    execute(interaction_dir='outputs/test')