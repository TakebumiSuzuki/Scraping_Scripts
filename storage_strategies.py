import io
import pathlib
from abc import ABC, abstractmethod
class StorageError(Exception):
    pass
class StorageFileNotFoundError(StorageError):
    pass
class StoragePermissionError(StorageError):
    pass

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
    #　def __init__(self, input_dir: str, output_dir: str):
    def __init__(self, output_path: pathlib.Path):
        self.output_path = output_path

    def save(self, string_io: io.StringIO, filename: str):
        try:
            full_path = self.output_path / filename
            print(f"Using LocalStorageStrategy to save to: '{full_path}'")

            # ディレクトリ作成
            full_path.parent.mkdir(parents=True, exist_ok=True)

            csv_content = string_io.getvalue()

            # ファイル書き込み
            with open(full_path, "w", newline="", encoding="utf-8") as f:
                f.write(csv_content)

            print(f"Successfully created '{full_path}'.")

        except PermissionError as e:
            # ディレクトリ作成やファイル書き込みの権限がない場合
            raise StoragePermissionError(f"Permission denied for local file: {full_path}") from e

        except IsADirectoryError as e:
            # 保存しようとしたパスが、ファイルではなく既存のディレクトリだった場合
            raise StorageError(f"Cannot save, path is a directory: {full_path}") from e

        except OSError as e:
            # ディスク容量不足、ファイル名が長すぎる、無効な文字が含まれるなど、
            # その他のOSレベルのI/Oエラーを捕捉
            raise StorageError(f"An OS error occurred while saving file: {full_path} ({e})") from e


    def read(self, filename: str) -> io.StringIO:
        full_path = self.output_path / filename
        print(f"LocalStorage: Reading '{filename}'.")
        try:
            # full_pathはpathlib.Pathオブジェクトで、.read_text()はそのオブジェクトが持つ便利なメソッド
            # .read_text()は、テキストファイルの中身を一度にすべて読み込み、1つの文字列として返すショートカットメソッド
            # 内部的には open()を使っている。読み込めるファイル: .csv, .txt, .json, .py, .html など。。
            # full_path.read_text() は、ファイルからテキストを読み込む際に、OSや元のファイルの改行コードの違いを吸収し、
            # Pythonのプログラム内では原則として改行を \n (ラインフィード, LF) という単一の文字に統一して扱います。
            # read_text() メソッドは、内部でファイルを開き、内容をすべて読み込んだ後、自動的にファイルをクローズします。
            content = full_path.read_text(encoding='utf-8')
            return io.StringIO(content)
        except FileNotFoundError as e:
            # FileNotFoundErrorを共通の例外に翻訳して再送出
            raise StorageFileNotFoundError(f"Local file not found: {full_path}") from e
        except PermissionError as e:
            # PermissionErrorを共通の例外に翻訳して再送出
            raise StoragePermissionError(f"Permission denied for local file: {full_path}") from e
        except IsADirectoryError as e:
            raise StorageError(f"Path is a directory, not a file: {full_path}") from e
        except UnicodeDecodeError as e:
            raise StorageError(f"Failed to decode file with UTF-8: {full_path}") from e
        except OSError as e:
            # その他のOS関連エラーをキャッチ
            raise StorageError(f"An OS error occurred while reading file: {full_path}") from e

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
        print(f"GCSStorage: Downloading '{filename}'.")
        # try:
        #     blob = self.bucket.blob(filename)
        #     content = blob.download_as_string().decode('utf-8')
        #     return io.StringIO(content)
        # except NotFound as e:
        #     # GCSのNotFound例外を共通の例外に翻訳
        #     raise StorageFileNotFoundError(f"File '{filename}' not found in GCS bucket '{self.bucket_name}'") from e
        # except Forbidden as e:
        #     # GCSのForbidden例外（権限エラー）を共通の例外に翻訳
        #     raise StoragePermissionError(f"Permission denied for GCS file '{filename}'") from e

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
    else:
        project_root = pathlib.Path(__file__).parent
        output_path = project_root / config['DEFAULT_OUTPUT_DIR']
        return LocalStorageStrategy(output_path=output_path)