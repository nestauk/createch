# Generic scripts to get DAPS tables

import logging
import os

import pandas as pd
from data_getters.core import get_engine

from createch import PROJECT_DIR


MYSQL_CONFIG = f"{PROJECT_DIR}/mysqldb_team.config"


def fetch_daps_table(table_name: str, path: str, fields: str = "all") -> pd.DataFrame:
    """Fetch DAPS tables if we don't have them already
    Args:
        table_name: name
        path: path for the table
        fields: fields to fetch. If a list, fetches those
    Returns:
        table
    """
    if os.path.join(path, f"{table_name}.csv") is True:
        logging.info(f"Already fetched {table_name}")
        return get_daps_table(table_name, path)
    else:
        logging.info(f"Fetching {table_name}")
        con = get_engine(MYSQL_CONFIG)

        if fields == "all":
            chunks = pd.read_sql_table(table_name, con, chunksize=1000)
        else:
            chunks = pd.read_sql_table(table_name, con, fields, chunksize=1000)
        return pd.concat(chunks)


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
