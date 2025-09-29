from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import google.api_core.exceptions
from google.cloud import aiplatform
import vertexai
from vertexai.language_models import TextEmbeddingModel

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
            raise

        if not all_chunks:
            logger.warning("No chunks found in the database. Nothing to process.")
            logger.info("--- Step 7: Finished (No data) ---")
            return

        logger.info(f"Successfully loaded {len(all_chunks)} chunks from the database.")

        # 2. Vertex AIの初期化とEmbeddingモデルのロード
        logger.info(f"Initializing Vertex AI for project '{config.GCP_PROJECT}' in region '{config.GCP_REGION}'...")
        vertexai.init(project=config.GCP_PROJECT, location=config.GCP_REGION)

        logger.info(f"Loading embedding model: '{config.EMBEDDING_MODEL_NAME}'...")
        model = TextEmbeddingModel.from_pretrained(config.EMBEDDING_MODEL_NAME)

        # 3. チャンクのコンテンツをバッチ処理でベクトル化
        logger.info(f"Generating embeddings for {len(all_chunks)} chunks...")

        all_texts = [chunk.content for chunk in all_chunks]
        all_ids = [str(chunk.id) for chunk in all_chunks]
        all_embeddings = []
        successful_chunk_indices = []  # 成功したチャンクのインデックスを記録

        # config.EMBEDDING_BATCH_SIZE ごとに分割してAPIを呼び出し、ベクトル化
        num_batches = (len(all_texts) - 1) // config.EMBEDDING_BATCH_SIZE + 1

        for i in range(0, len(all_texts), config.EMBEDDING_BATCH_SIZE):
            batch_texts = all_texts[i:i + config.EMBEDDING_BATCH_SIZE]
            current_batch_num = i // config.EMBEDDING_BATCH_SIZE + 1
            batch_start_idx = i
            logger.info(f"Generating embeddings for batch {current_batch_num}/{num_batches} ({len(batch_texts)} texts)...")

            try:
                embeddings = model.get_embeddings(batch_texts)
                all_embeddings.extend([emb.values for emb in embeddings])
                # 成功したチャンクのインデックスを記録
                successful_chunk_indices.extend(range(batch_start_idx, batch_start_idx + len(embeddings)))
                logger.info(f"Successfully generated embeddings for batch {current_batch_num}/{num_batches}")
            except google.api_core.exceptions.GoogleAPICallError as e:
                logger.error(f"Failed to generate embeddings for batch {current_batch_num}/{num_batches}: {e}", exc_info=True)
                logger.warning(f"Skipping batch {current_batch_num}/{num_batches} due to API error")
                continue
            except Exception as e:
                logger.error(f"Unexpected error while generating embeddings for batch {current_batch_num}/{num_batches}: {e}", exc_info=True)
                logger.warning(f"Skipping batch {current_batch_num}/{num_batches} due to unexpected error")
                continue

        if not all_embeddings:
            logger.error("No embeddings were successfully generated. Aborting upsert.")
            logger.info("--- Step 7: Finished with errors (No embeddings) ---")
            return

        logger.info(f"Successfully generated {len(all_embeddings)} embeddings out of {len(all_texts)} chunks.")

        # 4. Vector Search Indexに接続
        logger.info(f"Connecting to Vector Search Index: '{config.VECTOR_SEARCH_INDEX_ID}'...")
        try:
            my_index = aiplatform.MatchingEngineIndex(index_name=config.VECTOR_SEARCH_INDEX_ID)
            logger.info("Successfully connected to Vector Search Index.")
        except Exception as e:
            logger.error(f"Failed to connect to Vector Search Index: {e}", exc_info=True)
            raise

        # 5. データをVector Search Indexにバッチでアップサート
        logger.info(f"Upserting {len(all_embeddings)} vectors into the index...")

        # Vector Searchにアップサートするためのデータ形式のリストを作成（メタデータ付き）
        datapoints = []
        for idx, (chunk_idx, embedding) in enumerate(zip(successful_chunk_indices, all_embeddings)):
            chunk = all_chunks[chunk_idx]
            chunk_id = str(chunk.id)

            # メタデータの準備
            restricts = [
                {
                    "namespace": "scraped_at",
                    "allow_list": [chunk.scraped_at.strftime("%Y-%m-%d")]
                },
                {
                    "namespace": "scraped_at_timestamp",
                    "allow_list": [str(int(chunk.scraped_at.timestamp()))]
                }
            ]

            datapoint = {
                "datapoint_id": chunk_id,
                "feature_vector": embedding,
                "restricts": restricts
            }
            datapoints.append(datapoint)

        # config.VECTOR_SEARCH_UPSERT_BATCH_SIZE ごとに分割してアップサート
        num_upsert_batches = (len(datapoints) - 1) // config.VECTOR_SEARCH_UPSERT_BATCH_SIZE + 1

        for i in range(0, len(datapoints), config.VECTOR_SEARCH_UPSERT_BATCH_SIZE):
            batch_datapoints = datapoints[i:i + config.VECTOR_SEARCH_UPSERT_BATCH_SIZE]
            current_batch_num = i // config.VECTOR_SEARCH_UPSERT_BATCH_SIZE + 1
            logger.info(f"Upserting batch {current_batch_num}/{num_upsert_batches} ({len(batch_datapoints)} datapoints)...")

            try:
                my_index.upsert_datapoints(datapoints=batch_datapoints)
                logger.info(f"Successfully upserted batch {current_batch_num}/{num_upsert_batches}")
            except google.api_core.exceptions.GoogleAPICallError as e:
                logger.error(f"Failed to upsert batch {current_batch_num}/{num_upsert_batches}: {e}", exc_info=True)
                logger.warning(f"Skipping upsert for batch {current_batch_num}/{num_upsert_batches} due to API error")
                continue
            except Exception as e:
                logger.error(f"Unexpected error while upserting batch {current_batch_num}/{num_upsert_batches}: {e}", exc_info=True)
                logger.warning(f"Skipping upsert for batch {current_batch_num}/{num_upsert_batches} due to unexpected error")
                continue

        logger.info("Finished upserting vectors to Vector Search.")

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