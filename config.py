import os
from dotenv import load_dotenv

load_dotenv()

# Get the application environment from an environment variable.
# If not set, default to 'development'.
APP_ENV = os.environ.get('APP_ENV', 'development')

OUTPUT_BASE_DIR = 'outputs'

# You can add other configurations here later.
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'your-gcs-bucket-name')
SQLITE_DB_FILENAME = "scraped_data.sqlite"

# LOCAL_STORAGE_DIR = 'outputs'

# Timeout for network requests in seconds.
# We get it from environment variables to allow easy overrides.
# The value is converted to an integer.
TIMEOUT = int(os.environ.get('TIMEOUT', '30'))

# The filename contract between Step 1 and Step 2.
STEP1_OUTPUT_FILENAME = 'seed_urls_list.csv'
STEP2_OUTPUT_FILENAME = 'raw_urls_list.csv'
STEP3_OUTPUT_FILENAME = 'unique_urls_list.csv'

STEP5_OUTPUT_FILENAME = 'chunks.json'
# DATABASE_URL = "postgresql+psycopg2://postgres:your_password@localhost:5432/your_db"
DATABASE_URL = os.environ.get("DATABASE_URL")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

CHUNK_MIN_LENGTH = 300

CHUNK_MAX_LENGTH = 5000


GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION", "asia-northeast1") # デフォルト値を指定することも可能
VECTOR_SEARCH_INDEX_ID = os.environ.get("VECTOR_SEARCH_INDEX_ID")
EMBEDDING_MODEL_NAME = "text-embedding-004" # または "textembedding-gecko@003" など

# --- Batch Size Settings ---
# text-embedding-004モデルのget_embeddingsメソッドのバッチサイズ上限は250です
EMBEDDING_BATCH_SIZE = 250
# Vector Searchのupsert_datapointsメソッドの1リクエストあたりのデータポイント上限は1,000です
VECTOR_SEARCH_UPSERT_BATCH_SIZE = 1000


"""APP_ENV="development"
TIMEOUT=30
GCS_BUCKET_NAME=""
GCP_PROJECT="your-local-dev-gcp-project-id"
GCP_REGION="asia-northeast1"
VECTOR_SEARCH_INDEX_ID="your-dev-index-id"
DATABASE_URL="postgresql+psycopg2://postgres:your_password@localhost:5432/your_db"
"""