'''
cvsのリストの後ろから順に調べていって、除外する。
結果として850行の重複のないurlリスト(cfg.CLEANED_URLS_CSV)が出力される
'''

import csv
import io
import config
from storage_strategies import get_storage_strategy, StorageFileNotFoundError, StoragePermissionError

import logging, logging.config
from config_logging import LOGGING_CONFIG
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


APP_ENV = config.APP_ENV
GCS_BUCKET_NAME = ''
DEFAULT_OUTPUT_DIR = config.DEFAULT_OUTPUT_DIR
STEP2_FILENAME = config.STEP2_OUTPUT_FILENAME
STEP3_FILENAME = config.STEP3_OUTPUT_FILENAME


def process_rows(rows):
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

    logger.info("--- Step 3:  ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    app_config = {
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
        'DEFAULT_OUTPUT_DIR': DEFAULT_OUTPUT_DIR,
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

        processed_rows = process_rows(rows)
        # 処理結果をメモリ上のStringIOに書き出す
        string_io_output = io.StringIO()
        writer = csv.writer(string_io_output)
        writer.writerows(processed_rows)

        # storage.save()を使ってデータを保存する
        # 書き込み終わったStringIOオブジェクトを渡す
        storage.save(string_io_output, STEP3_FILENAME)

        logger.debug(f"処理が完了しました。結果は {STEP3_FILENAME} に保存されました。")

    except StorageFileNotFoundError as e: # ← ここを変更！
        logger.error(f"ファイルが見つかりません: {e}")
    except StoragePermissionError as e: # ← ここを変更！
        logger.error(f"ファイルへのアクセス権限がありません: {e}")
    except csv.Error as e:
        logger.error(f"CSVファイルの処理中にエラーが発生しました: {e}")
    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {e}", exc_info=True) # exc_info=True を付けると詳細なトレースバックがログに出力される
    else:
        logger.info('エラーなどなく、CSVファイルが作成されました')
        return

    logger.warning("エラーにより途中でプロセスを中止しました")
    return


if __name__ == "__main__":
    execute()