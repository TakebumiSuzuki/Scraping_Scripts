# step5_split_into_chunks_and_save/run.py

import io
import json
import re

from bs4 import BeautifulSoup
import html2text

# --- プロジェクトのルートパスをsys.pathに追加 ---
# import sys
# project_root = Path(__file__).resolve().parents[1]
# sys.path.insert(0, str(project_root))
# -----------------------------------------

import config
from storage_strategies import get_storage_strategy, StorageError
from config_logging import setup_logging

# --- Loggerの設定 ---
import logging
logger = logging.getLogger(__name__)

# --- 設定値の読み込み (config.pyから) ---
APP_ENV = config.APP_ENV
LOCAL_STORAGE_DIR = config.LOCAL_STORAGE_DIR
GCS_BUCKET_NAME = config.GCS_BUCKET_NAME
STEP5_OUTPUT_FILENAME = config.STEP5_OUTPUT_FILENAME
CHUNK_MIN_LENGTH = config.CHUNK_MIN_LENGTH
CHUNK_MAX_LENGTH = config.CHUNK_MAX_LENGTH


def clean_up_html(html_content: str) -> str:
    """
    BeautifulSoupを使い、HTMLから不要なタグの除去や構造の整理を行います。
    共有いただいた過去のスクリプトのロジックをほぼそのまま継承しています。
    """
    if not html_content:
        return ""

    # &nbsp; をスペースに置換
    html_content = re.sub(r'&nbsp;', ' ', html_content)
    soup = BeautifulSoup(html_content, 'html.parser')

    # 不要なカスタムタグや要素を削除
    for element in soup.find_all(['gkms-context-selector', 'img', 'iframe']):
        element.decompose()

    # クリックで展開される部分のヘッダーを<h3>に統一
    for el in soup.select('div.zippy-container > h2, div.zippy-container > a'):
        h3 = soup.new_tag('h3')
        h3.string = el.get_text(strip=True)
        el.replace_with(h3)

    # 意味を持たない<div>と<span>タグを削除（中のコンテンツは残す）
    for tag in soup.find_all(['div', 'span']):
        tag.unwrap()

    # tableタグの前に改行を挿入して、Markdown変換時のレンダリング崩れを防ぐ
    for table in soup.find_all('table'):
        br = soup.new_tag('br')
        table.insert_before(br)

    # 空のタグを削除
    for tag in soup(['a', 'p', 'h1', 'h2', 'h3', 'h4']):
        if not tag.get_text(strip=True):
            tag.decompose()

    # 3回以上の連続改行を2回にまとめる
    cleaned_html = str(soup)
    cleaned_html = re.sub(r'(\n[ \t]*){3,}', '\n\n', cleaned_html)

    return cleaned_html


def split_into_chunks(html_lines: list[str], md_converter: html2text.HTML2Text, max_len: int, min_len: int) -> list[str]:
    """
    HTMLの行リストを、見出しや文字数に基づいてチャンクに分割し、Markdownに変換します。
    """
    chunks = []
    current_chunk_html = ""
    # 見出しの階層を保持し、チャンクの先頭に追加することで文脈を維持します。
    header_context = {"h1": "", "h2": "", "h3": "", "h4": ""}

    for line in html_lines:
        # 見出しタグを検出してコンテキストを更新し、チャンクを分割する
        if '<h1' in line:
            if len(current_chunk_html) > min_len:
                chunks.append(md_converter.handle(current_chunk_html))
            current_chunk_html = ""
            header_context["h1"] = line
            header_context["h2"] = ""
            header_context["h3"] = ""
            header_context["h4"] = ""
        elif '<h2' in line:
            if len(current_chunk_html) > min_len:
                chunks.append(md_converter.handle(current_chunk_html))
            current_chunk_html = header_context["h1"]
            header_context["h2"] = line
            header_context["h3"] = ""
            header_context["h4"] = ""
        elif '<h3' in line:
            if len(current_chunk_html) > min_len:
                chunks.append(md_converter.handle(current_chunk_html))
            current_chunk_html = header_context["h1"] + header_context["h2"]
            header_context["h3"] = line
            header_context["h4"] = ""

        current_chunk_html += line

        # チャンクが最大長を超えたら分割する
        if len(current_chunk_html) > max_len:
            chunks.append(md_converter.handle(current_chunk_html))
            # 次のチャンクの先頭に、現在の見出しコンテキストと最後の行を追加
            current_chunk_html = "".join(header_context.values()) + line

    # 最後のチャンクを追加
    if current_chunk_html:
        chunks.append(md_converter.handle(current_chunk_html))

    return chunks


def add_metadata_and_finalize(chunks: list[str], url: str, category: str) -> list[dict]:
    """
    各チャンクにメタデータを付与し、最終的なJSONオブジェクトのリストを作成します。
    """
    final_chunks = []
    metadata_text = f"\n\n[SOURCE]: {url}\n[CATEGORY]: {category}\n\n"

    for chunk_content in chunks:
        # 余分な改行を整理
        content = re.sub(r'\n{3,}', '\n\n', chunk_content).strip()
        if not content:
            continue

        final_content = content + metadata_text
        final_chunks.append({"content": final_content})

    return final_chunks



def execute():
    """Main execution function for step 5."""
    logger.info("--- Step 5: Starting HTML Chunking and Saving ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    try:
        # 1. 入力ストレージ戦略を取得 (step4のコンテキストを指定)
        config_dict = {
            'LOCAL_STORAGE_DIR': LOCAL_STORAGE_DIR,
            'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
        }
        input_storage = get_storage_strategy(APP_ENV, config_dict, step_context='step4')
        logger.info(f"Using input storage: '{input_storage.__class__.__name__}'")

        # 出力ストレージ戦略を取得 (step5のコンテキスト = デフォルト)
        output_storage = get_storage_strategy(APP_ENV, config_dict)
        logger.info(f"Using output storage: '{output_storage.__class__.__name__}'")

        # 2. html2textコンバータを初期化
        md_converter = html2text.HTML2Text()
        md_converter.ignore_links = True
        md_converter.body_width = 0

        all_chunks_list = []
        page_count = 0

        # 3. イテレータを使って全ページをメモリ効率よく処理
        page_iterator = input_storage.get_storage_iterator()

        for category, url, html_content, scraped_at in page_iterator:
            page_count += 1
            logger.info(f"Processing page {page_count}: {url}")

            if not html_content:
                logger.warning(f"Skipping page with empty content: {url}")
                continue

            # 3a. HTMLクリーニング、チャンク化、メタデータ付与
            cleaned_html = clean_up_html(html_content)
            html_lines = cleaned_html.split('\n')
            md_chunks = split_into_chunks(html_lines, md_converter, CHUNK_MAX_LENGTH, CHUNK_MIN_LENGTH)
            final_chunks = add_metadata_and_finalize(md_chunks, url, category)

            all_chunks_list.extend(final_chunks)
            logger.debug(f"Generated {len(final_chunks)} chunks for this page.")

        # 4. 結果をJSONとして保存
        if not all_chunks_list:
            logger.warning("No chunks were generated. Nothing to save.")
            logger.info("--- Step 5: Finished (No output) ---")
            return

        logger.info(f"Processed {page_count} pages and generated a total of {len(all_chunks_list)} chunks.")
        logger.info(f"Preparing to save chunks to '{STEP5_OUTPUT_FILENAME}'...")

        json_string = json.dumps(all_chunks_list, ensure_ascii=False, indent=2)
        json_io = io.StringIO(json_string)

        output_storage.save(json_io, filename=STEP5_OUTPUT_FILENAME)
        logger.info(f"Successfully saved all chunks to '{STEP5_OUTPUT_FILENAME}'.")

    except (StorageError, NotImplementedError) as e:
        logger.critical(f"A storage-related error occurred: {e}", exc_info=True)
        logger.info("--- Step 5: Finished with errors ---")
        return
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}", exc_info=True)
        logger.info("--- Step 5: Finished with errors ---")
        return

    logger.info("--- Step 5: Finished successfully ---")


if __name__ == "__main__":
    setup_logging()
    execute()