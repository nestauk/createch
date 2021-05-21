# Fetch GtR tables
# TODO: Add organisations and funding tables

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
        projects.rename(columns={"id": "project_id"})
        .merge(link.query("table_name=='gtr_funds'"), on="project_id")
        .merge(funders[["id", "start"]], on="id")
        .drop_duplicates(subset=["project_id"])
        .assign(year=lambda df: df["start"].map(lambda x: x.year))
        .query("year>2006")
        .reset_index(drop=True)
    )
    return projects_filt


def fetch_save_gtr_tables():
    projects = fetch_daps_table("gtr_projects", GTR_PATH, PROJECT_FIELDS)
    funders = fetch_daps_table("gtr_funds", GTR_PATH)
    topics = fetch_daps_table("gtr_topic", GTR_PATH)
    link = fetch_daps_table("gtr_link_table", GTR_PATH)

    projects_filtered = filter_projects(projects, link, funders)

    save_daps_table(projects_filtered, "gtr_projects", GTR_PATH)
    save_daps_table(topics, "gtr_topic", GTR_PATH)
    save_daps_table(funders, "gtr_funds", GTR_PATH)
    save_daps_table(link, "gtr_link_table", GTR_PATH)


if __name__ == "__main__":
    if os.path.exists(GTR_PATH) is False:
        os.mkdir(GTR_PATH)

    fetch_save_gtr_tables()
