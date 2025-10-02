import sys
import os
import csv
import config

from storage_strategies import get_storage_strategy, StorageFileNotFoundError, StoragePermissionError
from utils import convert_rows_to_in_memory_csv

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)

APP_ENV = config.APP_ENV
OUTPUT_BASE_DIR = config.OUTPUT_BASE_DIR
STEP2_FILENAME = config.STEP2_OUTPUT_FILENAME
STEP3_FILENAME = config.STEP3_OUTPUT_FILENAME


def _remove_duplicate_rows_by_url(rows):
    """Removes duplicate rows based on the URL, keeping the last occurrence."""
    seen_urls = set()
    unique_rows = []
    # reversed()はPythonの組み込み関数で、リストやタプルなどのシーケンス（順番を持つデータ構造）を
    # 受け取り、その要素を逆順に取り出すイテレーターを返す。で、イテレーターの仕組み上、最初から順番にしか
    # 取り出せないので、どうしても後ろの方のエントリーを優先的にキープするという仕様を貫くならば、
    # 以下のような実装になり、イテレータによるメモリを節約するという方法は使えない。
    for row in reversed(rows):
        if len(row) > 1:
            url = row[1]
            if url not in seen_urls:
                seen_urls.add(url)
                unique_rows.append(row)
            else:
                logger.info(f"Duplicate URL found, will not include in the new file: {url}")
    return list(reversed(unique_rows))


def execute(interaction_dir):

    logger.info("--- Step 3: Removing Duplicate URLs and Saving the unique URLs List ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    storage = get_storage_strategy(APP_ENV, interaction_dir)
    logger.info(f"Using storage strategy: '{storage.__class__.__name__}'")

    try:
        logger.info(f"Loading URLs from '{STEP2_FILENAME}'...")
        # LocalStorageを使っている場合には、csvからioに読み込み、再度csvに戻すという無駄が発生しているが、
        # これは、GCS Storageを使った時との統一的な扱い、抽象化するための無駄。
        string_io_input = storage.read(STEP2_FILENAME)
        reader = csv.reader(string_io_input)
        rows = list(reader)
        logger.info(f"Successfully loaded {len(rows)} rows.")

        processed_rows = _remove_duplicate_rows_by_url(rows)
        logger.info(f"Successfully processed {len(processed_rows)} unique rows.")

        logger.info(f"Converting {len(processed_rows)} unique rows to in-memory CSV...")
        string_io_output = convert_rows_to_in_memory_csv(processed_rows)
        logger.debug("In-memory CSV buffer created successfully.")

        logger.info(f"Saving unique rows to '{STEP3_FILENAME}'...")
        storage.save(string_io_output, STEP3_FILENAME)
        logger.info(f"Successfully saved {len(processed_rows)} rows to '{STEP3_FILENAME}'.")

    except Exception as e:
        if isinstance(e, StorageFileNotFoundError):
            logger.error(f"File not found: {e}")
        elif isinstance(e, StoragePermissionError):
            logger.error(f"File permission error: {e}")
        elif isinstance(e, csv.Error):
            logger.error(f"Error processing CSV file: {e}")
        else:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        logger.info("--- Step 3: Finished with errors ---")
        return

    logger.info("--- Step 3: Finished successfully ---")


if __name__ == "__main__":
    # コマンドライン引数が存在すれば、それで上書きする
    if len(sys.argv) > 1:
        run_id_arg = sys.argv[1]
        interaction_dir = os.path.join(OUTPUT_BASE_DIR, run_id_arg)
    else:
        interaction_dir = os.path.join(OUTPUT_BASE_DIR, 'test')

    setup_logging(base_dir=interaction_dir)
    execute(interaction_dir)