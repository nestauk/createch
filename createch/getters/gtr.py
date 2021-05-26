# Fetch GtR tables
import json
import logging
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


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CreatechNestaGetter/{run_id}")


def get_name() -> Dict[str, str]:
    """Lookup between GtR organisation ID and name."""
    return _flow(RUN_ID).data.gtr_names
