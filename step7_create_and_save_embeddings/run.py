# pip install google-cloud-aiplatform google-cloud-vertexai

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Vertex AI SDKのインポート
import google.api_core.exceptions
from google.cloud import aiplatform
from vertexai.language_models import TextEmbeddingModel
from google.cloud.aiplatform import IndexDatapoint

import config
from models.Chunk import Chunk

import logging
from config_logging import setup_logging
logger = logging.getLogger(__name__)


def execute():
    """
    データベースからチャンクを読み込み、Vertex AIでベクトル化し、
    Vertex AI Vector Searchにアップサートするメイン関数。
    """
    logger.info("--- Step 7: Starting Vectorization and Upsert to Vector Search ---")

    try:
        # 1. データベースから全てのチャンクを取得
        logger.info("Connecting to the database to fetch chunks...")
        try:
            engine = create_engine(config.DATABASE_URL)
            Session = sessionmaker(bind=engine)
            with Session() as session:
                all_chunks = session.query(Chunk).all()
        except SQLAlchemyError as e:
            logger.error("Failed to connect to or read from the database.", exc_info=True)
            raise  # エラーを再送出してメインのexceptブロックで捕捉させる

        if not all_chunks:
            logger.warning("No chunks found in the database. Nothing to process.")
            logger.info("--- Step 7: Finished (No data) ---")
            return

        logger.info(f"Successfully loaded {len(all_chunks)} chunks from the database.")

        # 2. Vertex AIの初期化とEmbeddingモデルのロード
        logger.info(f"Initializing Vertex AI for project '{config.GCP_PROJECT}' in region '{config.GCP_REGION}'...")
        aiplatform.init(project=config.GCP_PROJECT, location=config.GCP_REGION)

        logger.info(f"Loading embedding model: '{config.EMBEDDING_MODEL_NAME}'...")
        model = TextEmbeddingModel.from_pretrained(config.EMBEDDING_MODEL_NAME)

        # 3. チャンクのコンテンツをバッチ処理でベクトル化
        logger.info(f"Generating embeddings for {len(all_chunks)} chunks...")

        all_texts = [chunk.content for chunk in all_chunks]
        all_ids = [str(chunk.id) for chunk in all_chunks]  # Vector SearchのIDは文字列である必要があります
        all_embeddings = []

        # config.EMBEDDING_BATCH_SIZE ごとに分割してAPIを呼び出し、ベクトル化
        for i in range(0, len(all_texts), config.EMBEDDING_BATCH_SIZE):
            batch_texts = all_texts[i:i + config.EMBEDDING_BATCH_SIZE]
            num_batches = (len(all_texts) - 1) // config.EMBEDDING_BATCH_SIZE + 1
            current_batch_num = i // config.EMBEDDING_BATCH_SIZE + 1
            logger.info(f"Generating embeddings for batch {current_batch_num}/{num_batches}...")

            embeddings_result = model.get_embeddings(batch_texts)
            all_embeddings.extend([embedding.values for embedding in embeddings_result])

        logger.info(f"Successfully generated {len(all_embeddings)} embeddings.")

        # 4. Vector Search Indexに接続
        logger.info(f"Connecting to Vector Search Index: '{config.VECTOR_SEARCH_INDEX_ID}'...")
        my_index = aiplatform.MatchingEngineIndex(index_name=config.VECTOR_SEARCH_INDEX_ID)

        # 5. データをVector Search Indexにバッチでアップサート
        logger.info(f"Upserting {len(all_embeddings)} vectors into the index...")

        # Vector Searchにアップサートするためのデータ形式(IndexDatapoint)のリストを作成
        datapoints = [
            IndexDatapoint(datapoint_id=chunk_id, feature_vector=embedding)
            for chunk_id, embedding in zip(all_ids, all_embeddings)
        ]

        # config.VECTOR_SEARCH_UPSERT_BATCH_SIZE ごとに分割してアップサート
        for i in range(0, len(datapoints), config.VECTOR_SEARCH_UPSERT_BATCH_SIZE):
            batch_datapoints = datapoints[i:i + config.VECTOR_SEARCH_UPSERT_BATCH_SIZE]
            num_batches = (len(datapoints) - 1) // config.VECTOR_SEARCH_UPSERT_BATCH_SIZE + 1
            current_batch_num = i // config.VECTOR_SEARCH_UPSERT_BATCH_SIZE + 1
            logger.info(f"Upserting batch {current_batch_num}/{num_batches}...")
            my_index.upsert_datapoints(datapoints=batch_datapoints)

        logger.info("Successfully upserted all vectors to Vector Search.")

    except (SQLAlchemyError, google.api_core.exceptions.GoogleAPICallError) as e:
        logger.critical(f"A critical error occurred with the database or Google Cloud API: {e}", exc_info=True)
        logger.info("--- Step 7: Finished with errors ---")
        return
    except Exception as e:
        logger.critical(f"An unexpected critical error occurred: {e}", exc_info=True)
        logger.info("--- Step 7: Finished with errors ---")
        return

    logger.info("--- Step 7: Finished successfully ---")


if __name__ == "__main__":
    setup_logging()
    execute()