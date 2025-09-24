import io
import pathlib
from abc import ABC, abstractmethod
import sqlite3
from collections.abc import Iterator #これはコード内で戻り値に対しイテレーター型を型表記で使うため

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

    @abstractmethod
    def get_storage_iterator(self) -> Iterator[tuple]:
        """
        Returns an iterator that yields all stored items one by one.
        Each item should be a tuple, e.g., (category, url, content).
        """
        pass

# --- Concrete Strategy for Local Storage ---
class LocalStorageStrategy(StorageStrategy):
    """Saves the content to a local file."""
    def __init__(self, local_storage_path: pathlib.Path):
        self.local_storage_path = local_storage_path

    def save(self, string_io: io.StringIO, filename: str, metadata: dict | None = None):
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

    # ★インターフェースを実装するが、このクラスの責務ではないためNotImplementedErrorを発生させる
    def get_storage_iterator(self) -> Iterator[tuple]:
        raise NotImplementedError(
            "LocalStorageStrategy for simple files does not support iterating over structured page data."
        )



# --- Concrete Strategy for GCS ---
class GCSStorageStrategy(StorageStrategy):
    """Saves the content to a Google Cloud Storage bucket."""
    def __init__(self, bucket_name: str):
        # self.client = storage.Client()
        # self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name # For demonstration
        print(f"Using GCSStorageStrategy. Target bucket: '{self.bucket_name}'")

    def save(self, string_io: io.StringIO, filename: str, metadata: dict | None = None):
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

    def get_storage_iterator(self) -> Iterator[tuple]:
        """
        [Simulation] In a real scenario, this would use `bucket.list_blobs()`
        to iterate through all stored HTML files and download them one by one.
        """
        print("GCSStorage: Streaming all pages from bucket. (Simulated)")
        # ダミーデータをyieldで返す
        dummy_pages = [
            ("GCS Category 1", "https://example.com/gcs/page1", "<h1>GCS Page 1</h1><p>Content here.</p>"),
            ("GCS Category 2", "https://example.com/gcs/page2", "<h1>GCS Page 2</h1><p>More content.</p><div><img src='...' /></div>"),
            ("GCS Category 1", "https://example.com/gcs/page3", "<h1>GCS Page 3</h1><h2>Subtitle</h2><p>Final text.</p>"),
        ]
        for page_data in dummy_pages:
            yield page_data


class SQLiteStorageStrategy(StorageStrategy):
    """Saves and reads content to/from a local SQLite database."""
    def __init__(self, db_path: pathlib.Path):
        self.db_path = db_path
        self._conn = None
        try:
            # parents=Trueの場合、途中の親ディレクトリが存在しなくても、再帰的にすべての親ディレクトリを作成します。
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            # self.db_pathにファイルがまだ存在しない場合は、この時点で新しく作成される
            # check_same_thread=False を設定すると、複数のスレッドから同じデータベース接続を共有できるようになる。
            # この場合、開発者自身がスレッドセーフティ（排他制御など）を考慮する必要がある。
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._create_table()
        except sqlite3.OperationalError as e:
            raise StoragePermissionError(f"Could not connect to or create database at '{self.db_path}': {e}") from e
        except OSError as e:
            raise StoragePermissionError(f"Could not create directory for database at '{self.db_path.parent}': {e}") from e

    def _create_table(self):
        """Ensures the table exists with the required schema."""
        # ★ご要望のスキーマ + scraped_at に更新
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS scraped_pages (
            id INTEGER PRIMARY KEY,
            category TEXT,
            reference_url TEXT UNIQUE NOT NULL,
            content TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
        """
        try:
            # カーソルは一つだけではなく、conn.cursor()を呼び出すたびに新しいカーソルオブジェクトが生成されます。
            # そして、一般的にはSQLを実行する処理の都度、新しくカーソルを取得し、使い捨てる
            cursor = self._conn.cursor()
            cursor.execute(create_table_sql)
            self._conn.commit()
        except sqlite3.Error as e:
            raise StorageError(f"Failed to create table in database '{self.db_path}': {e}") from e

    def save(self, string_io: io.StringIO, filename: str, metadata: dict | None = None):
        """
        Saves content to the database.
        Uses 'filename' as the URL and extracts 'category' from metadata.
        If the URL already exists, it updates the existing record.
        """
        html_content = string_io.getvalue()
        url = filename
        category = metadata.get('category', '') if metadata else ''

        # reference_urlがUNIQUE制約を持つため、ON CONFLICTでUPDATEする
        # ON CONFLICT構文を使うことにより、一つのSQLクエリでアトミックに upseart 作業が行える。　
        # CURRENT_TIMESTAMP はSQLite側で実行される関数であり、UTC（協定世界時）で日時を保存します。
        # SQLite自体には専用の日時型がなく、TIMESTAMPとしてテーブルを定義しても、文字列として扱います。
        sql = """
        INSERT INTO scraped_pages (reference_url, category, content, scraped_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(reference_url) DO UPDATE SET
            category=excluded.category,
            content=excluded.content,
            scraped_at=excluded.scraped_at;
        """
        try:
            print(f"Using SQLiteStorageStrategy to save URL: '{url}' with category: '{category}'")
            cursor = self._conn.cursor()
            cursor.execute(sql, (url, category, html_content))
            self._conn.commit()
            print(f"Successfully saved/updated '{url}' in '{self.db_path}'.")
        except sqlite3.Error as e:
            self._conn.rollback()
            raise StorageError(f"Failed to save to SQLite database: {e}") from e

    def read(self, filename: str) -> io.StringIO:
        """Reads HTML content from the database using the URL as a key."""
        url = filename
        sql = "SELECT content FROM scraped_pages WHERE reference_url = ?;"
        try:
            print(f"SQLiteStorage: Reading '{url}'.")
            cursor = self._conn.cursor()
            cursor.execute(sql, (url,))
            result = cursor.fetchone()
            if result:
                return io.StringIO(result[0])
            else:
                raise StorageFileNotFoundError(f"URL not found in SQLite database: {url}")
        except sqlite3.Error as e:
            raise StorageError(f"Failed to read from SQLite database: {e}") from e

    def exists(self, filename: str) -> bool:
        """Checks if a record for the given URL exists in the database."""
        url = filename
        sql = "SELECT 1 FROM scraped_pages WHERE reference_url = ?;"
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, (url,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            print(f"An error occurred while checking existence in SQLite: {e}")
            return False

    def close(self):
        """Closes the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            print(f"SQLite connection to '{self.db_path}' closed.")

    # この関数が呼ばれると、rowを返すのではなく、この関数から作られるジェネレーターインスタンスがメモリ上に展開され、返される。
    def get_storage_iterator(self) -> Iterator[tuple]:
        """
        Connects to the database and yields all pages one by one to save memory.
        The connection is managed within this generator.
        """
        if not self.db_path.exists():
            raise StorageFileNotFoundError(f"Database file not found: {self.db_path}")

        conn = None
        # ここで with構文は使えない。なぜなら、sqlite3でのwithは、トランザクションの管理を自動的に行ってくれるためのものなので。
        try:
            conn = sqlite3.connect(self.db_path)
            #  SQLiteのデータベース内では、TEXT 型のデータは特定のエンコーディング（通常はUTF-8）のバイト列として保存されています。
            # スクレイピングで取得したHTMLデータには、さまざまな理由（文字化け、不正な文字コードの混入など）で、UTF-8として正しくデコードできないバイト列が含まれていることがよくあり
            #conn.text_factory は、この「バイト列 → 文字列」の変換ルールをプログラマが自由にカスタマイズできる機能
            # バイト列 b を（デフォルトのUTF-8で）デコードしなさい。
            # ただし、もしデコードできない不正なバイトが見つかっても、エラーを発生させるのではなく、その不正なバイトを просто無視（ignore）して処理を続けてください。
            conn.text_factory = lambda b: b.decode(errors='ignore')
            cursor = conn.cursor()
            query = "SELECT category, reference_url, content, scraped_at FROM scraped_pages ORDER BY id"

            print(f"SQLiteStorage: Streaming pages from '{self.db_path}'...")
            cursor.execute(query)

            for row in cursor:
                yield row # (category, reference_url, content)

        except sqlite3.Error as e:
            raise StorageError(f"Failed to stream from SQLite database: {e}") from e
        finally:
            if conn:
                conn.close()


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
        db_path = project_root / config['LOCAL_STORAGE_DIR'] / 'scraped_data.sqlite'
        return SQLiteStorageStrategy(db_path=db_path)
    else:
        # それ以外のステップ(step1-3)では、従来通りローカルファイルシステムを使う
        project_root = pathlib.Path(__file__).parent
        local_storage_path = project_root / config['LOCAL_STORAGE_DIR']
        return LocalStorageStrategy(local_storage_path=local_storage_path)