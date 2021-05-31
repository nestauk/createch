# Fetch GtR tables
# TODO: Add organisations and funding tables

import json
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
    projects = fetch_daps_table("gtr_projects", PROJECT_FIELDS)
    funders = fetch_daps_table("gtr_funds")
    topics = fetch_daps_table("gtr_topic")
    link = fetch_daps_table("gtr_link_table")

    projects_filtered = filter_projects(projects, link, funders)

    save_daps_table(projects_filtered, "gtr_projects", GTR_PATH)
    save_daps_table(topics, "gtr_topic", GTR_PATH)
    save_daps_table(funders, "gtr_funds", GTR_PATH)
    save_daps_table(link, "gtr_link_table", GTR_PATH)


if __name__ == "__main__":
    if os.path.exists(GTR_PATH) is False:
        os.mkdir(GTR_PATH)

    fetch_save_gtr_tables()
