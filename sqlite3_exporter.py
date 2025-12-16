import sqlite3
import json
from pathlib import Path
from typing import Any, Dict, List, Union
from urllib.parse import urlparse
import gzip


def sqlite_url_to_path(url: str) -> Path:
    return Path(urlparse(url).path)


def sqlite_table_to_json(
    db_path: Union[str, Path],
    table_name: str,
    output_path: Union[str, Path],
) -> None:
    """
    Export the given SQLite table to a JSON file with structure:

        {
          "<table_name>": [
            {col1: val1, col2: val2, ...},
            ...
          ]
        }

    Parameters
    ----------
    db_path :
        Path to the SQLite database file.
    table_name :
        Name of the table to export.
    output_path :
        Path to the JSON file to write.

    Notes
    -----
    The table name is safely quoted as an SQL identifier.
    """
    db_path = Path(db_path)
    output_path = Path(output_path)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        # bezpečné ocitování jména tabulky
        quoted_table = '"' + table_name.replace('"', '""') + '"'

        cur.execute(f"SELECT * FROM {quoted_table}")
        columns = [d[0] for d in cur.description]

        rows: List[Dict[str, Any]] = [
            dict(zip(columns, row))
            for row in cur.fetchall()
        ]

        data = {table_name: rows}
        output_path = output_path.with_suffix(output_path.suffix + ".gz")

        # zápis JSON do souboru
        with gzip.open(output_path, "wt", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    finally:
        conn.close()