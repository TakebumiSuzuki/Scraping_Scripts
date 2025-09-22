import io
import pathlib
from abc import ABC, abstractmethod
import sqlite3
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
    def __init__(self, local_storage_path: pathlib.Path):
        self.local_storage_path = local_storage_path

    def save(self, string_io: io.StringIO, filename: str):
        try:
            full_path = self.local_storage_path / filename
            print(f"Using LocalStorageStrategy to save to: '{full_path}'")

            full_path.parent.mkdir(parents=True, exist_ok=True)

            csv_content = string_io.getvalue()

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
        full_path = self.local_storage_path / filename
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
        try:
            full_path = self.local_storage_path / filename
            return full_path.exists()
        except PermissionError:
            # 権限エラーで確認できない場合は「存在しない」として扱うか、
            # もしくはログを出力するなど、アプリケーションの要件に応じて対応する。
            # ここではシンプルに False を返す例を示す。
            print(f"Permission denied while checking existence of '{full_path}'.")
            return False
        except OSError as e:
            # その他のOSエラーが発生した場合も同様
            print(f"An OS error occurred while checking existence: {e}")
            return False



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


class SQLiteStorageStrategy(StorageStrategy):
    """Saves and reads content to/from a local SQLite database."""

    def __init__(self, db_path: pathlib.Path):
        """
        Initializes the SQLite storage strategy.

        Args:
            db_path: The path to the SQLite database file.
        """
        self.db_path = db_path
        self._conn = None
        try:
            # データベースファイルが置かれるディレクトリを作成
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._create_table()
        except sqlite3.OperationalError as e:
            # ディレクトリのパーミッションがない、ディスクが書き込み不可などの場合に発生
            raise StoragePermissionError(f"Could not connect to or create database at '{self.db_path}': {e}") from e
        except OSError as e:
            # mkdirでOSレベルのエラーが発生した場合
            raise StoragePermissionError(f"Could not create directory for database at '{self.db_path.parent}': {e}") from e


    def _create_table(self):
        """Ensures the 'pages' table exists in the database."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS pages (
            url TEXT PRIMARY KEY,
            html_content TEXT NOT NULL,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(create_table_sql)
            self._conn.commit()
        except sqlite3.Error as e:
            raise StorageError(f"Failed to create table in database '{self.db_path}': {e}") from e


    def save(self, string_io: io.StringIO, filename: str):
        """
        Saves HTML content to the database. The filename is used as the URL.
        If the URL already exists, its content and timestamp are updated.
        """
        html_content = string_io.getvalue()
        url = filename # このコンテキストではfilenameがURLとなる

        # INSERT OR REPLACE は、主キー(url)が既存の場合はUPDATE、存在しない場合はINSERTを実行します。
        sql = """
        INSERT OR REPLACE INTO pages (url, html_content, scraped_at)
        VALUES (?, ?, CURRENT_TIMESTAMP);
        """
        try:
            print(f"Using SQLiteStorageStrategy to save URL: '{url}'")
            cursor = self._conn.cursor()
            cursor.execute(sql, (url, html_content))
            self._conn.commit()
            print(f"Successfully saved/updated '{url}' in '{self.db_path}'.")
        except sqlite3.Error as e:
            self._conn.rollback()
            raise StorageError(f"Failed to save to SQLite database: {e}") from e


    def read(self, filename: str) -> io.StringIO:
        """Reads HTML content from the database using the URL as a key."""
        url = filename
        sql = "SELECT html_content FROM pages WHERE url = ?;"
        try:
            print(f"SQLiteStorage: Reading '{url}'.")
            cursor = self._conn.cursor()
            cursor.execute(sql, (url,))
            result = cursor.fetchone() # (html_content,) というタプル or None

            if result:
                return io.StringIO(result[0])
            else:
                raise StorageFileNotFoundError(f"URL not found in SQLite database: {url}")
        except sqlite3.Error as e:
            raise StorageError(f"Failed to read from SQLite database: {e}") from e


    def exists(self, filename: str) -> bool:
        """Checks if a record for the given URL exists in the database."""
        url = filename
        # 存在確認は COUNT(*) よりも SELECT 1 の方が一般的に高速です
        sql = "SELECT 1 FROM pages WHERE url = ?;"
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, (url,))
            result = cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            # エラー発生時は「存在しない」として扱う方が安全な場合が多い
            print(f"An error occurred while checking existence in SQLite: {e}")
            return False

    def close(self):
        """Closes the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            print(f"SQLite connection to '{self.db_path}' closed.")


# --- Factory Function ---
def get_storage_strategy(
    env: str,
    config: dict,
    step_context: str = 'default' # デフォルト値を設定
) -> StorageStrategy:
    """
    Factory function to select the appropriate storage strategy.
    """
    if env == 'production':
        # 本番は常にGCSなので変更なし
        return GCSStorageStrategy(bucket_name=config['GCS_BUCKET_NAME'])

    # --- 開発環境の場合の分岐 ---
    if step_context == 'step4':
        # step4の開発環境ではSQLiteを使う
        project_root = pathlib.Path(__file__).parent
        db_path = project_root / config['LOCAL_STORAGE_DIR'] / 'scraped_pages.sqlite'
        return SQLiteStorageStrategy(db_path=db_path)
    else:
        # それ以外のステップ(step1-3)では、従来通りローカルファイルシステムを使う
        project_root = pathlib.Path(__file__).parent
        local_storage_path = project_root / config['LOCAL_STORAGE_DIR']
        return LocalStorageStrategy(local_storage_path=local_storage_path)