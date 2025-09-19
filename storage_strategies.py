import io
import pathlib
from abc import ABC, abstractmethod

# To use GCS, you need to install the library:
# pip install google-cloud-storage
# from google.cloud import storage

# --- Base Strategy Interface ---
class StorageStrategy(ABC):
    @abstractmethod
    def save(self, string_io: io.StringIO, filename: str):
        """Saves content from a string buffer to the storage."""
        pass

    @abstractmethod
    def read(self, filename: str) -> io.StringIO:
        """Reads content from the storage into a string buffer."""
        pass

    @abstractmethod
    def exists(self, filename: str) -> bool:
        """Checks if a file exists in the storage."""
        pass

# --- Concrete Strategy for Local Storage ---
class LocalStorageStrategy(StorageStrategy):
    """Saves the content to a local file."""
    def __init__(self, output_dir: str):
        self.output_path = pathlib.Path(output_dir)

    def save(self, string_io: io.StringIO, filename: str):
        full_path = self.output_path / filename
        print(f"Using LocalStorageStrategy to save to: '{full_path}'")

        full_path.parent.mkdir(parents=True, exist_ok=True)
        csv_content = string_io.getvalue()

        with open(full_path, "w", newline="", encoding="utf-8") as f:
            f.write(csv_content)
        print(f"Successfully created '{full_path}'.")

    def read(self, filename: str) -> io.StringIO:
        full_path = self.output_path / filename
        print(f"LocalStorage: Reading '{filename}'.")
        content = full_path.read_text(encoding='utf-8')
        return io.StringIO(content)

    def exists(self, filename: str) -> bool:
        full_path = self.output_path / filename
        return full_path.exists()


# --- Concrete Strategy for GCS ---
class GCSStorageStrategy(StorageStrategy):
    """Saves the content to a Google Cloud Storage bucket."""
    def __init__(self, bucket_name: str):
        # self.client = storage.Client()
        # self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name # For demonstration
        print(f"Using GCSStorageStrategy. Target bucket: '{self.bucket_name}'")

    def save(self, string_io: io.StringIO, filename: str):
        # NOTE: This is a placeholder for the actual GCS upload logic.
        # You would uncomment the lines in __init__ and implement the upload here.
        # blob = self.bucket.blob(filename)
        csv_content = string_io.getvalue()
        # blob.upload_from_string(csv_content, content_type='text/csv')

        print(f"Successfully uploaded '{filename}' to GCS bucket '{self.bucket_name}'.")
        print("(This is a simulation. Actual GCS upload is commented out.)")

    def read(self, filename: str) -> io.StringIO:
        print(f"GCSStorage: Downloading '{filename}'. (Simulated)")
        # blob = self.bucket.blob(filename)
        # content = blob.download_as_string().decode('utf-8')
        # return io.StringIO(content)
        # --- シミュレーション用のダミーデータ ---
        dummy_content = "url\nhttps://example.com/from/gcs"
        return io.StringIO(dummy_content)

    def exists(self, filename: str) -> bool:
        # blob = self.bucket.blob(filename)
        # return blob.exists()
        print(f"GCSStorage: Checking existence of '{filename}'. (Simulated)")
        return True # シミュレーション


# --- Factory Function ---
def get_storage_strategy(env: str, config: dict) -> StorageStrategy:
    """
    Factory function to select the appropriate storage strategy based on the environment.
    """
    if env == 'production':
        return GCSStorageStrategy(bucket_name=config['GCS_BUCKET_NAME'])
    else: # 'development' or any other value
        project_root = pathlib.Path(__file__).parent
        output_dir = project_root / config['DEFAULT_OUTPUT_DIR']
        return LocalStorageStrategy(output_dir=str(output_dir))