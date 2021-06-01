# Get and enrich the CB and GTR matches
import logging
import os
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests

from createch import PROJECT_DIR
from createch.getters.companies_house import get_address, get_sector
from createch.getters.jacchammer import get_crunchbase, get_gtr

NSPL_PATH = f"{PROJECT_DIR}/inputs/data/nspl"


def fetch_nspl():
    """Fetch NSPL if needed"""

    if os.path.exists(NSPL_PATH) is False:
        logging.info("Fetching NSPL")
        nspl_req = requests.get(
            "https://www.arcgis.com/sharing/rest/content/items/7606baba633d4bbca3f2510ab78acf61/data"
        )
        zipf = ZipFile(BytesIO(nspl_req.content))
        zipf.extractall(path=NSPL_PATH)
    else:
        logging.info("Already fetched nspl")


def make_nspl_to_merge():
    """Merge NSPL postcodes with TTWA names"""

    nspl = pd.read_csv(
        f"{NSPL_PATH}/Data/NSPL_FEB_2021_UK.csv", usecols=["pcds", "ttwa"]
    )
    ttwa = pd.read_csv(
        f"{NSPL_PATH}/Documents/TTWA names and codes UK as at 12_11 v5.csv"
    )
    nspl = (
        nspl.merge(ttwa, left_on="ttwa", right_on="TTWA11CD")
        .drop(axis=1, labels=["TTWA11CD"])
        .rename(columns={"ttwa": "ttwa_code", "TTWA11NM": "ttwa_name"})
    )

    return nspl


def enrich_source(
    source: pd.DataFrame, ch_tables: list, threshold: int = 70
) -> pd.DataFrame:
    """Enrich a source-company table with sector and address info
    Args:
        source: a lookup between source companies and companies house
        ch_tables: list with company addresses and sectors
        threshold: threshold for matching
    Returns:
        A df enriched with sector and location (TTWA)

    """
    nspl = make_nspl_to_merge()

    enriched = (
        source.query(f"sim_mean>={threshold}")
        .merge(
            ch_tables[0][["company_number", "postcode"]],
            on="company_number",
            how="inner",
        )
        .merge(
            ch_tables[1].query("rank==1")[["company_number", "SIC4_code"]], how="inner"
        )
        .merge(nspl, left_on="postcode", right_on="pcds")
    )
    return enriched


if __name__ == "__main__":

    fetch_nspl()

    logging.info("Getting data")
    ch_add = get_address()
    ch_sect = get_sector()

    logging.info("Enriching data")
    gtr_enr, cb_enr = [
        enrich_source(getter(), [ch_add, ch_sect])
        for getter in [get_gtr, get_crunchbase]
    ]

    logging.info("Saving enriched tables")
    gtr_enr.to_csv(
        f"{PROJECT_DIR}/inputs/data/gtr/gtr_ch_organisations.csv", index=False
    )
    cb_enr.to_csv(
        f"{PROJECT_DIR}/inputs/data/crunchbase/crunchbase_ch_organisations.csv",
        index=False,
    )
