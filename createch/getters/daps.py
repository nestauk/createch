import logging
import os
from configparser import ConfigParser

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


load_dotenv(find_dotenv())

MYSQL_CONFIG = os.getenv("MYSQL_CONFIG")


def get_engine(config_path, database="production", **engine_kwargs):
    """Get a SQL alchemy engine from config"""
    cp = ConfigParser()
    cp.read(config_path)
    cp = cp["client"]
    url = URL(
        drivername="mysql+pymysql",
        database=database,
        username=cp["user"],
        host=cp["host"],
        password=cp["password"],
    )
    return create_engine(url, **engine_kwargs)


def get_daps_table(name: str, path: str) -> pd.DataFrame:
    """Get DAPS table
    Args:
        name: table name
        path: storage path
    """
    table = pd.read_csv(f"{path}/{name}.csv")
    return table


def save_daps_table(table: pd.DataFrame, name: str, path: str):
    """Save DAPS tables
    Args:
        table: table to save
        name: table name
        path: directory where we store the table
    """
    table.to_csv(f"{path}/{name}.csv", index=False)


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
    con = get_engine(MYSQL_CONFIG)

    if fields == "all":
        chunks = pd.read_sql_table(table_name, con, chunksize=1000)
    else:
        chunks = pd.read_sql_table(table_name, con, columns=fields, chunksize=1000)
    return pd.concat(chunks)
