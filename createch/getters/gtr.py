# Fetch GtR tables
import json
import logging
import os
from functools import lru_cache
from typing import Dict

import pandas as pd
from metaflow import namespace, Run

import createch
from createch import config, PROJECT_DIR
from createch.pipeline.fetch_daps1_data.gtr_utils import GTR_PATH

logger = logging.getLogger(__name__)

namespace(None)

RUN_ID: int = createch.config["flows"]["nesta"]["run_id"]


def get_gtr_projects():
    df = pd.read_csv(f"{GTR_PATH}/gtr_projects.csv")
    df["start"] = pd.to_datetime(df["start"])
    return df


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


def get_organisations(creative=True):

    SIC_IND_LOOKUP = get_cis_lookup()
    FILTER_TERMS = config["gtr_organisations"]["filter_terms"]

    orgs = pd.read_csv(
        f"{PROJECT_DIR}/inputs/data/gtr/gtr_ch_organisations.csv",
        dtype={"SIC4_code": str},
    )

    orgs = orgs[
        [
            "gtr_id",
            "gtr_name",
            "company_number",
            "ch_name",
            "SIC4_code",
            "ttwa_code",
            "ttwa_name",
            "sim_mean",
        ]
    ].drop_duplicates("gtr_id")
    orgs["flag"] = [
        any(x in name.lower() for x in FILTER_TERMS) for name in orgs["gtr_name"]
    ]

    orgs = orgs.query("flag==False").assign(
        creative_sector=lambda df: df["SIC4_code"].map(SIC_IND_LOOKUP)
    )

    if creative is True:
        orgs = orgs.dropna(axis=0, subset=["creative_sector"]).reset_index(drop=True)
    else:
        orgs = orgs.query("flag==False").fillna(value={"creative_sector": "Other"})

    logging.info("Expanding gtr organisations")
    orgs_final = expand_gtr_orgs(orgs)

    return orgs_final


def get_gtr_topics():
    return pd.read_csv(f"{PROJECT_DIR}/outputs/data/gtr/gtr_topic_mix.csv", index_col=0)


def get_gtr_disciplines():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/gtr/predicted_disciplines.csv", index_col=0
    )


def get_project_orgs_lookup():
    link = get_link_table().query("table_name=='gtr_organisations'")

    return link[["id", "project_id"]]


def get_gtr_createch_tagged():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/gtr_createch_tagged.csv", index_col=0
    )


def get_gtr_orgs_tagged():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/gtr/gtr_createch_orgs.csv",
        dtype={"SIC4_code": str},
    )


def expand_gtr_orgs(orgs):
    """Expands GTR organisations with other potentially relevant ones"""
    gtr_addresses = fetch_daps_table("gtr_organisations_locations")
    uk_orgs = set(
        gtr_addresses.loc[gtr_addresses["country_name"] == "United Kingdom"]["id"]
    )

    gtr_orgs = fetch_daps_table("gtr_organisations")
    gtr_orgs_uk = gtr_orgs.loc[gtr_orgs["id"].isin(uk_orgs)].reset_index(drop=True)

    relevant = ["museum", "library", "gallery", "broadcasting", "bbc"]
    museum = ["museum", "library", "gallery"]

    gtr_orgs_rel = gtr_orgs_uk.loc[
        [any(r in name.lower() for r in relevant) for name in gtr_orgs_uk["name"]]
    ]
    gtr_orgs_rel = gtr_orgs_rel.loc[~gtr_orgs_rel["id"].isin(orgs["gtr_id"])]

    gtr_orgs_rel["creative_sector"] = [
        "Museums galleries and libraries"
        if any(m in name.lower() for m in ["museum", "library", "gallery"])
        else "Film TV video radio and photography"
        for name in gtr_orgs_rel["name"]
    ]

    gtr_orgs_rel = gtr_orgs_rel.rename(
        columns={"id": "gtr_id", "name": "gtr_name"}
    ).drop(axis=1, labels=["addresses"])

    # Reassign any musesums to the museum sector
    orgs_final = pd.concat([orgs, gtr_orgs_rel])
    orgs_final["creative_sector"] = [
        "Museums galleries and libraries"
        if any(r in row["gtr_name"].lower() for r in museum)
        else row["creative_sector"]
        for _, row in orgs_final.iterrows()
    ]

    return orgs_final


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

    if creative is True:
        orgs = orgs.dropna(axis=0, subset=["creative_sector"]).reset_index(drop=True)
    else:
        orgs = orgs.query("flag==False").fillna(value={"creative_sector": "Other"})

    logging.info("Expanding gtr organisations")
    orgs_final = expand_gtr_orgs(orgs)

    return orgs_final


def get_gtr_topics():
    return pd.read_csv(f"{PROJECT_DIR}/outputs/data/gtr/gtr_topic_mix.csv", index_col=0)


def get_gtr_disciplines():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/gtr/predicted_disciplines.csv", index_col=0
    )


def get_project_orgs_lookup():
    link = get_link_table().query("table_name=='gtr_organisations'")

    return link[["id", "project_id"]]


def get_gtr_createch_tagged():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/gtr_createch_tagged.csv", index_col=0
    )


def get_gtr_orgs_tagged():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/gtr/gtr_createch_orgs.csv",
        dtype={"SIC4_code": str},
    )


def expand_gtr_orgs(orgs):
    """Expands GTR organisations with other potentially relevant ones"""
    gtr_addresses = fetch_daps_table("gtr_organisations_locations")
    uk_orgs = set(
        gtr_addresses.loc[gtr_addresses["country_name"] == "United Kingdom"]["id"]
    )

    gtr_orgs = fetch_daps_table("gtr_organisations")
    gtr_orgs_uk = gtr_orgs.loc[gtr_orgs["id"].isin(uk_orgs)].reset_index(drop=True)

    relevant = ["museum", "library", "gallery", "broadcasting", "bbc"]
    museum = ["museum", "library", "gallery"]

    gtr_orgs_rel = gtr_orgs_uk.loc[
        [any(r in name.lower() for r in relevant) for name in gtr_orgs_uk["name"]]
    ]
    gtr_orgs_rel = gtr_orgs_rel.loc[~gtr_orgs_rel["id"].isin(orgs["gtr_id"])]

    gtr_orgs_rel["creative_sector"] = [
        "Museums galleries and libraries"
        if any(m in name.lower() for m in ["museum", "library", "gallery"])
        else "Film TV video radio and photography"
        for name in gtr_orgs_rel["name"]
    ]

    gtr_orgs_rel = gtr_orgs_rel.rename(
        columns={"id": "gtr_id", "name": "gtr_name"}
    ).drop(axis=1, labels=["addresses"])

    # Reassign any musesums to the museum sector
    orgs_final = pd.concat([orgs, gtr_orgs_rel])
    orgs_final["creative_sector"] = [
        "Museums galleries and libraries"
        if any(r in row["gtr_name"].lower() for r in museum)
        else row["creative_sector"]
        for _, row in orgs_final.iterrows()
    ]

    return orgs_final


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CreatechNestaGetter/{run_id}")


def get_name() -> Dict[str, str]:
    """Lookup between GtR organisation ID and name."""
    return _flow(RUN_ID).data.gtr_names
