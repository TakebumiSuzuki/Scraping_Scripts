import os

# Get the application environment from an environment variable.
# If not set, default to 'development'.
APP_ENV = os.environ.get('APP_ENV', 'development')

# You can add other configurations here later.
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'your-gcs-bucket-name')

DEFAULT_OUTPUT_DIR = 'outputs'

# Timeout for network requests in seconds.
# We get it from environment variables to allow easy overrides.
# The value is converted to an integer.
TIMEOUT = int(os.environ.get('TIMEOUT', '30'))

# The filename contract between Step 1 and Step 2.
STEP1_OUTPUT_FILENAME = 'seed_urls.csv'