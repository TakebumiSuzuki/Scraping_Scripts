import io
import json
import logging
from datetime import datetime

from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base
from models.Chunk import Chunk

Base = declarative_base()

import config
from storage_strategies import get_storage_strategy, StorageError


# --- Loggerの設定 ---
# config_logging.pyが存在することを前提としています
from config_logging import setup_logging
logger = logging.getLogger(__name__)

# --- 設定値の読み込み (config.pyから) ---
APP_ENV = config.APP_ENV
STEP5_OUTPUT_FILENAME = config.STEP5_OUTPUT_FILENAME
DATABASE_URL = config.DATABASE_URL


# pip install sqlalchemy psycopg2-binary を行うこと
def execute(interaction_dir):
    """Main execution function for step 6."""
    logger.info("--- Step 6: Starting Chunk Saving to Database ---")
    logger.info(f"Running in '{APP_ENV}' environment.")

    try:
        # 1. step5の出力ファイル(chunks.json)を読み込むためのストレージ戦略を取得
        # step5は単純なファイルとして保存するため、step_contextはデフォルトのまま
        input_storage = get_storage_strategy(APP_ENV, interaction_dir)
        logger.info(f"Using input storage: '{input_storage.__class__.__name__}'")

        # 2. chunks.jsonを読み込み、Pythonオブジェクトに変換
        logger.info(f"Reading chunks from '{STEP5_OUTPUT_FILENAME}'...")
        json_io = input_storage.read(STEP5_OUTPUT_FILENAME)
        all_chunks_list = json.load(json_io)

        if not all_chunks_list:
            logger.warning("No chunks found in the input file. Nothing to save to the database.")
            logger.info("--- Step 6: Finished (No data) ---")
            return

        logger.info(f"Successfully loaded {len(all_chunks_list)} chunks from the file.")

        # 3. データベースへの接続設定
        logger.info("Connecting to the database...")
        engine = create_engine(DATABASE_URL)

        # テーブルが存在しない場合は作成する
        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)

        # 4. データベースへの保存処理
        # withブロックを使うことで、セッションのクローズが自動的に行われる
        with Session() as session:
            try:
                # 冪等性（再実行可能性）を確保するため、これから挿入するデータのIDを先に取得
                ids_to_insert = [chunk['id'] for chunk in all_chunks_list]

                # 該当するIDを持つ既存のレコードを先に削除する
                logger.info(f"Deleting {len(ids_to_insert)} existing chunks to prevent duplicates...")
                # .where()句に合致する行がない場合、このdelete文は何も実行しない
                session.execute(delete(Chunk).where(Chunk.id.in_(ids_to_insert)))

                # scraped_atを文字列からdatetimeオブジェクトに変換
                for chunk in all_chunks_list:
                    # fromisoformatはタイムゾーン情報を含むISO 8601形式の文字列を正しくパースできる
                    chunk['scraped_at'] = datetime.fromisoformat(chunk['scraped_at'])

                # bulk_insert_mappingsを使って高速にデータを挿入
                logger.info(f"Inserting {len(all_chunks_list)} new chunks into the database...")
                session.bulk_insert_mappings(Chunk, all_chunks_list)

                # トランザクションをコミット
                session.commit()
                logger.info(f"Successfully saved {len(all_chunks_list)} chunks to the database.")

            except SQLAlchemyError as e:
                logger.error("An error occurred during the database transaction. Rolling back...", exc_info=True)
                session.rollback()
                # エラーを再送出し、上位のexceptブロックで捕捉させる
                raise StorageError(f"Failed to save data to the database: {e}") from e


    except (StorageError, NotImplementedError) as e:
        logger.critical(f"A storage-related error occurred: {e}", exc_info=True)
        logger.info("--- Step 6: Finished with errors ---")
        return
    except json.JSONDecodeError as e:
        logger.critical(f"Failed to parse '{STEP5_OUTPUT_FILENAME}'. It might be corrupted or not a valid JSON file: {e}", exc_info=True)
        logger.info("--- Step 6: Finished with errors ---")
        return
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}", exc_info=True)
        logger.info("--- Step 6: Finished with errors ---")
        return

    logger.info("--- Step 6: Finished successfully ---")


if __name__ == "__main__":
    setup_logging()
    execute(interaction_dir='outputs/test')