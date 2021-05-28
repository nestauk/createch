# Generic scripts to get DAPS tables

import logging
from typing import Iterator


import pandas as pd
from data_getters.core import get_engine
from pandas._typing import FilePathOrBuffer

from createch import PROJECT_DIR


MYSQL_CONFIG = f"{PROJECT_DIR}/mysqldb_team.config"


def fetch_daps_table(table_name: str, fields: str = "all") -> pd.DataFrame:
    """Fetch DAPS tables if we don't have them already
    Args:
        table_name: name
        path: path for the table
        fields: fields to fetch. If a list, fetches those
    Returns:
        table
    """
    logging.info(f"Fetching {table_name}")
    engine = get_engine(MYSQL_CONFIG)
    con = engine.connect().execution_options(stream_results=True)

    if fields == "all":
        chunks = pd.read_sql_table(table_name, con, chunksize=1000)
    else:
        chunks = pd.read_sql_table(table_name, con, columns=fields, chunksize=1000)

    return chunks


def stream_df_to_csv(
    df_iterator: Iterator[pd.DataFrame],
    path_or_buf: FilePathOrBuffer,
    **kwargs,
):
    """Stream a DataFrame iterator to csv.

    Args:
        df_iterator: DataFrame chunks to stream to CSV
        path_or_buf: FilePath or Buffer (passed to `DataFrame.to_csv`)
        kwargs: Extra args passed to `DataFrame.to_csv`. Cannot contain
            any of `{"mode", "header", "path_or_buf"}` - `mode` is "a" and
            `header` is `False` for all but initial chunks.


    Raises:
        ValueError if `kwargs` contains disallowed values.
    """
    if any((key in kwargs for key in ["mode", "header", "path_or_buf"])):
        raise ValueError()

    # First chunk: mode "w" and write column names
    initial = next(df_iterator)
    initial.to_csv(path_or_buf, **kwargs)
    # Subsequent chunks:
    for chunk in df_iterator:
        chunk.to_csv(path_or_buf, mode="a", header=False, **kwargs)


def save_daps_table(table: pd.DataFrame, name: str, path: str):
    """Save DAPS tables
    Args:
        table: table to save
        name: table name
        path: directory where we store the table
    """
    table.to_csv(f"{path}/{name}.csv", index=False)


def get_daps_table(name: str, path: str) -> pd.DataFrame:
    """Get DAPS table
    Args:
        name: table name
        path: storage path
    """
    table = pd.read_csv(f"{path}/{name}.csv")
    return table
