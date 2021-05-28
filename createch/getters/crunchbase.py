import logging
import os
from functools import lru_cache
from typing import Dict

import pandas as pd
from metaflow import namespace, Run

import createch
from createch import PROJECT_DIR
from createch.getters.daps import fetch_daps_table, save_daps_table

logger = logging.getLogger(__name__)
namespace(None)

RUN_ID: int = createch.config["flows"]["nesta"]["run_id"]

CB_PATH = f"{PROJECT_DIR}/inputs/data/crunchbase"


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CreatechNestaGetter/{run_id}")


def get_name() -> Dict[str, str]:
    """Lookup between Crunchbase organisation ID and name."""
    return _flow(RUN_ID).data.crunchbase_names


def filter_uk(table: pd.DataFrame, ids: set, var_name: str = "org_id"):
    """Gets UK companies from crunchbase
    Args:
        table: crunchbase table
        ids: UK company ids
        var_name: name of org id variable
    Returns:
        filtered table
    """
    return table.loc[table[var_name].isin(ids)].reset_index(drop=True)


def fetch_save_crunchbase():
    """Fetch and save crunchbase data"""
    cb_orgs = pd.concat(fetch_daps_table("crunchbase_organizations", fields="all"))

    cb_uk = cb_orgs.loc[cb_orgs["country"] == "United Kingdom"].drop_duplicates(
        subset=["id"]
    )
    logging.info(len(cb_uk))
    save_daps_table(cb_uk, "crunchbase_organisations", CB_PATH)

    cb_uk_ids = set(cb_uk["id"])

    cb_funding_rounds = pd.concat(
        fetch_daps_table("crunchbase_funding_rounds", fields="all")
    )
    cb_funding_rounds_uk = filter_uk(cb_funding_rounds, cb_uk_ids)

    cb_orgs_cats = pd.concat(
        fetch_daps_table("crunchbase_organizations_categories", fields="all")
    )
    cb_org_cats_uk = filter_uk(cb_orgs_cats, cb_uk_ids, "organization_id")

    category_group = pd.concat(
        fetch_daps_table("crunchbase_category_groups", fields="all")
    )

    save_daps_table(cb_funding_rounds_uk, "crunchbase_funding_rounds", CB_PATH)
    save_daps_table(cb_org_cats_uk, "crunchbase_organizations_categories", CB_PATH)
    save_daps_table(category_group, "crunchbase_category_groups", CB_PATH)
