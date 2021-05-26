# Fetch GtR tables
import json
import logging
import os
from typing import Iterator

import pandas as pd
from data_getters.core import get_engine

from createch import config, PROJECT_DIR
from createch.getters.daps import (
    fetch_daps_table,
    MYSQL_CONFIG,
    stream_df_to_csv,
)

GTR_PATH = os.path.join(PROJECT_DIR, "inputs/data/gtr")

PROJECT_FIELDS = [
    "id",
    "title",
    "grantCategory",
    "leadFunder",
    "abstractText",
    "techAbstractText",
]
FUNDER_FIELDS = ["id", "start", "category", "amount"]


def get_gtr_projects():
    return pd.read_csv(f"{GTR_PATH}/gtr_projects.csv")


def get_gtr_predicted_disciplines():
    pd.read_csv(f"{PROJECT_DIR}/outputs/data/gtr/predicted_disciplines.csv")


def get_gtr_tokenised():
    with open(f"{PROJECT_DIR}/outputs/data/gtr/gtr_tokenised.json", "r") as infile:
        return json.load(infile)


def get_gtr_tagged():
    with open(f"{PROJECT_DIR}/outputs/data/gtr/gtr_area_tagged.json", "r") as infile:
        return json.load(infile)


def get_gtr_vocabulary():
    with open(f"{PROJECT_DIR}/outputs/data/gtr/gtr_vocabulary.json", "r") as infile:
        return json.load(infile)


def get_cis_lookup():
    lookup = pd.read_csv(
        f"{PROJECT_DIR}/inputs/data/sic_sector_lookup.csv", dtype={"SIC4_code": str}
    )
    code_industry_lu = (
        lookup.query("industry != 'Life Sciences'")
        .set_index("SIC4_code")["industry"]
        .to_dict()
    )
    return code_industry_lu


def get_link_table():
    return pd.read_csv(f"{PROJECT_DIR}/inputs/data/gtr/gtr_link_table.csv")


def get_organisations():

    SIC_IND_LOOKUP = get_cis_lookup()
    FILTER_TERMS = config["gtr_organisations"]["filter_terms"]

    orgs = pd.read_csv(
        "~/Desktop/projects/im-minerva/data/processed/gtr/projects_organisations.csv",
        dtype={"SIC4_code": str},
    )

    orgs = orgs[["id", "name", "company_number", "score", "SIC4_code"]].drop_duplicates(
        "id"
    )
    orgs["flag"] = [
        any(x in name.lower() for x in FILTER_TERMS) for name in orgs["name"]
    ]

    orgs = (
        orgs.query("flag==False")
        .assign(creative_sector=lambda df: df["SIC4_code"].map(SIC_IND_LOOKUP))
        .dropna(axis=0, subset=["creative_sector"])
        .reset_index(drop=True)
    )
    return orgs


def projects_funded_from_2006() -> Iterator[pd.DataFrame]:
    """GtR projects with funding starting from 2006.

    Returns:
        Iterable of query results
    """
    engine = get_engine(MYSQL_CONFIG)
    con = engine.connect().execution_options(stream_results=True)
    query = """
    SELECT
        DISTINCT gtr_projects.id AS project_id,
        gtr_projects.title,
        gtr_projects.grantCategory,
        gtr_projects.leadFunder,
        gtr_projects.abstractText,
        gtr_projects.potentialImpact,
        gtr_projects.techAbstractText,
        gtr_funds.start
    FROM
        gtr_projects
            INNER JOIN
                (SELECT * FROM gtr_link_table
                 WHERE gtr_link_table.table_name = 'gtr_funds')
                AS l
                ON l.project_id = gtr_projects.id
            INNER JOIN
                (SELECT * FROM gtr_funds
                 WHERE YEAR(gtr_funds.start) > 2006)
                AS gtr_funds
                ON l.id = gtr_funds.id
    GROUP BY gtr_projects.id HAVING MIN(YEAR(gtr_funds.start));
    """

    return pd.read_sql_query(query, con, chunksize=1000)


def fetch_save_gtr_tables():

    funders = fetch_daps_table("gtr_funds")
    stream_df_to_csv(funders, path_or_buf=f"{GTR_PATH}/gtr_funds.csv", index=False)

    topics = fetch_daps_table("gtr_topic")
    stream_df_to_csv(topics, f"{GTR_PATH}/gtr_topics.csv", index=False)

    link = fetch_daps_table("gtr_link_table")
    stream_df_to_csv(link, f"{GTR_PATH}/gtr_link_table.csv", index=False)

    logging.info("Filtering projects...")
    projects_filtered = projects_funded_from_2006()
    stream_df_to_csv(projects_filtered, f"{GTR_PATH}/gtr_projects.csv", index=False)


if __name__ == "__main__":
    if os.path.exists(GTR_PATH) is False:
        os.mkdir(GTR_PATH)

    fetch_save_gtr_tables()
