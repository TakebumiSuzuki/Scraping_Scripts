import csv
import config

from storage_strategies import get_storage_strategy, StorageFileNotFoundError, StoragePermissionError
from utils import convert_rows_to_in_memory_csv

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)


APP_ENV = config.APP_ENV
LOCAL_STORAGE_DIR = config.LOCAL_STORAGE_DIR
GCS_BUCKET_NAME = config.GCS_BUCKET_NAME
STEP2_FILENAME = config.STEP2_OUTPUT_FILENAME
STEP3_FILENAME = config.STEP3_OUTPUT_FILENAME


def remove_duplicate_rows_by_url(rows):
    seen_urls = set()
    unique_rows = []
    for row in reversed(rows):
        if len(row) > 1:
            url = row[1]
            if url not in seen_urls:
                seen_urls.add(url)
                unique_rows.append(row) # Claudeによると、大規模なcsvの場合メモリ節約のため、この部分をgeneratorにしてもいいとの事。
            else:
                logger.info(f'このurlは重複しているため、新しいファイルには含めません: {url}')
    return list(reversed(unique_rows))


def execute():

    logger.info("--- Step 3: Removing Duplicate URLs and Saving the unique URLs List ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    app_config = {
        'LOCAL_STORAGE_DIR': LOCAL_STORAGE_DIR,
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
    }
    storage = get_storage_strategy(APP_ENV, app_config)

    try:
        logger.info(f"Loading URLs from '{STEP2_FILENAME}'...")
        # LocalStorageを使っている場合には、csvからioに読み込み、もう一度csvに治すという無駄が発生しているが、
        # GCS Storageを使った時との統一的な扱い、抽象化するための無駄。
        string_io_input = storage.read(STEP2_FILENAME)
        reader = csv.reader(string_io_input)
        rows = list(reader)
        logger.info(f"Successfully loaded {len(rows)} rows.")

        processed_rows = remove_duplicate_rows_by_url(rows)
        logger.info(f"Successfully processed {len(processed_rows)} unique rows.")

        # --- ここからが修正部分 ---
        logger.info(f"Converting {len(processed_rows)} unique rows to in-memory CSV...")
        string_io_output = convert_rows_to_in_memory_csv(processed_rows)
        logger.debug("In-memory CSV buffer created successfully.")

        logger.info(f"Saving unique rows to '{STEP3_FILENAME}'...")
        storage.save(string_io_output, STEP3_FILENAME)
        logger.info(f"Successfully saved {len(processed_rows)} rows to '{STEP3_FILENAME}'.")

    except StorageFileNotFoundError as e: # ← ここを変更！
        logger.error(f"ファイルが見つかりません: {e}")
        return
    except StoragePermissionError as e: # ← ここを変更！
        logger.error(f"ファイルへのアクセス権限がありません: {e}")
        return
    except csv.Error as e:
        logger.error(f"CSVファイルの処理中にエラーが発生しました: {e}")
        return
    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {e}", exc_info=True) # exc_info=True を付けると詳細なトレースバックがログに出力される

    logger.info("--- Step 3: Finished successfully ---")


if __name__ == "__main__":
    setup_logging()
    execute()