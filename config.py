import os

# Get the application environment from an environment variable.
# If not set, default to 'development'.
APP_ENV = os.environ.get('APP_ENV', 'development')

# You can add other configurations here later.
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'your-gcs-bucket-name')

LOCAL_STORAGE_DIR = 'outputs'

# Timeout for network requests in seconds.
# We get it from environment variables to allow easy overrides.
# The value is converted to an integer.
TIMEOUT = int(os.environ.get('TIMEOUT', '30'))

# The filename contract between Step 1 and Step 2.
STEP1_OUTPUT_FILENAME = 'seed_urls_list.csv'
STEP2_OUTPUT_FILENAME = 'raw_urls_list.csv'
STEP3_OUTPUT_FILENAME = 'unique_urls_list.csv'

STEP5_OUTPUT_FILENAME = 'chunks.json'

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
]

CHUNK_MIN_LENGTH = 300

CHUNK_MAX_LENGTH = 5000