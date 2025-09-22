import io
import csv

def convert_rows_to_in_memory_csv(data_rows: list[list[str]]) -> io.StringIO:
    """
    Takes a list of rows (where each row is a list of strings)
    and converts it into an in-memory CSV file object.

    Args:
        data_rows: A list of lists, where each inner list represents a row.

    Returns:
        An io.StringIO object containing the CSV data.
    """
    string_io = io.StringIO()
    writer = csv.writer(string_io)

    # writerowはリストを受け取るので、そのまま渡すだけ
    writer.writerows(data_rows)

    # io.StringIO オブジェクトに書き込みを行うと、カーソル（現在の位置）が末尾に移動します。このバッファを後で
    # storage_saver.save で読み込む際に、カーソルが末尾にあると何も読み込めません。
    string_io.seek(0)
    return string_io