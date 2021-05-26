# Fetch GtR tables
# TODO: Add organisations and funding tables
import gc
import logging
import os

import pandas as pd

from createch import PROJECT_DIR
from createch.getters.daps import fetch_daps_table, save_daps_table

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


def collect():
    gc.collect()
    return


def filter_link(link):
    return link.loc[lambda link: link.table_name == "gtr_funds"]


def filter_projects(
    projects: pd.DataFrame, link: pd.DataFrame, funders: pd.DataFrame
) -> pd.DataFrame:
    """Merges GTR projects with funding information and filters by year
    Args:
        projects: project_table
        link: link between projects and other gtr metadata
        funders: funder information including project start date
    Returns:
        Expanded and filtered project list
    """
    projects_filt = (
        link.loc[lambda link: link.table_name == "gtr_funds"]
        .merge(
            funders[["id", "start"]]
            .assign(year=lambda df: df["start"].map(lambda x: x.year))
            .loc[lambda funders: funders.year > 2006, ("id", "year")],
            on="id",
        )
        .merge(
            projects.rename(columns={"id": "project_id"}).drop_duplicates(
                subset=["project_id"]
            ),
            on="project_id",
        )
        .reset_index(drop=True)
    )

    return projects_filt


def fetch_save_gtr_tables():

    funders = fetch_daps_table("gtr_funds")
    save_daps_table(funders, "gtr_funds", GTR_PATH)

    topics = fetch_daps_table("gtr_topic")
    save_daps_table(topics, "gtr_topic", GTR_PATH)

    link = fetch_daps_table("gtr_link_table")
    save_daps_table(link, "gtr_link_table", GTR_PATH)

    # To reduce memory-usage:
    # - shadowing `link` variable to get only data needed next
    # - run garbage collector
    link = filter_link(link)
    collect()

    logging.info("Filtering projects...")
    projects = fetch_daps_table("gtr_projects", PROJECT_FIELDS)
    projects_filtered = filter_projects(projects, link, funders)
    save_daps_table(projects_filtered, "gtr_projects", GTR_PATH)


if __name__ == "__main__":
    if os.path.exists(GTR_PATH) is False:
        os.mkdir(GTR_PATH)

    fetch_save_gtr_tables()
