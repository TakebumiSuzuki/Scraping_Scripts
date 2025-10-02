import sys
import os
import json
from datetime import datetime
import uuid

from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from database import Base
from models.Chunk import Chunk

import config
from storage_strategies import get_storage_strategy, StorageError

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)

APP_ENV = config.APP_ENV
OUTPUT_BASE_DIR = config.OUTPUT_BASE_DIR
STEP5_OUTPUT_FILENAME = config.STEP5_OUTPUT_FILENAME
DATABASE_URL = config.DATABASE_URL


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
        # ここでJSONファイルを辞書のリストに変換するが、日時を表す文字列はdatetimeオブジェクトに変換されず、文字列のまま。
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
        # Base.metadata.create_all(engine)

        # Webアプリケーションの各リクエストや、各ワーカースレッドは、それぞれが自分専用の Session を
        # sessionmakerから生成して使い、処理が終わったらクローズ（session.close()）する
        Session = sessionmaker(bind=engine)

        # 4. データベースへの保存処理
        with Session() as session:
            try:
                """
                以下は、upsertのような作業を行う場合。
                # 冪等性（再実行可能性）を確保するため、これから挿入するデータのIDを先に取得して削除。
                # 冪等性[べきとうせい]（Idempotence）とは、「ある操作を何回実行しても、結果が常に同じになる」という性質のこと
                # もし、何らかの理由（例えば、一度成功したのにネットワークエラーで成功通知が受け取れず、再実行してしまったなど）
                # でこのプログラムが複数回実行された場合でも、データが重複しないようにする
                ids_to_insert = [chunk['id'] for chunk in all_chunks_list]

                # 該当するIDを持つ既存のレコードを先に削除する
                logger.info(f"Deleting {len(ids_to_insert)} existing chunks to prevent duplicates...")
                # .where()句に合致する行がない場合、このdelete文は何も実行しない
                session.execute(delete(Chunk).where(Chunk.id.in_(ids_to_insert)))
                """

                # 以下は、upsertではなく、全件削除して、全て情報を入れ替える場合
                logger.info("Deleting all existing chunks to make the table clean...")
                session.execute(delete(Chunk))

                # scraped_atを文字列からdatetimeオブジェクトに変換
                for chunk in all_chunks_list:
                    # chunk['id'] = uuid.UUID(chunk['id'])

                    # fromisoformatはタイムゾーン情報を含むISO 8601形式の文字列を正しくパースできる
                    chunk['scraped_at'] = datetime.fromisoformat(chunk['scraped_at'])

                # bulk_insert_mappingsを使って高速にデータを挿入
                logger.info(f"Inserting {len(all_chunks_list)} new chunks into the database...")
                # all_chunks_list 辞書のキーは、Chunkモデルで定義された属性名と一致している必要があります。
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
    # コマンドライン引数が存在すれば、それで上書きする
    if len(sys.argv) > 1:
        run_id_arg = sys.argv[1]
        interaction_dir = os.path.join(OUTPUT_BASE_DIR, run_id_arg)
    else:
        interaction_dir = os.path.join(OUTPUT_BASE_DIR, 'test')

    setup_logging(base_dir=interaction_dir)
    execute(interaction_dir)