import os

import pandas as pd

from createch import PROJECT_DIR
from createch.getters.daps import fetch_daps_table, save_daps_table

CB_PATH = f"{PROJECT_DIR}/inputs/data/crunchbase"

if os.path.exists(CB_PATH) is False:
    os.makedirs(CB_PATH)


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
    cb_orgs = fetch_daps_table("crunchbase_organizations", CB_PATH, fields="all")

    cb_uk = cb_orgs.loc[cb_orgs["country"] == "United Kingdom"]
    cb_uk_ids = set(cb_uk["id"])

    cb_funding_rounds = fetch_daps_table(
        "crunchbase_funding_rounds", CB_PATH, fields="all"
    )
    cb_funding_rounds_uk = filter_uk(cb_funding_rounds, cb_uk_ids)

    cb_orgs_cats = fetch_daps_table(
        "crunchbase_organizations_categories", CB_PATH, fields="all"
    )
    cb_org_cats_uk = filter_uk(cb_orgs_cats, cb_uk_ids, "organization_id")

    category_group = fetch_daps_table(
        "crunchbase_category_groups", CB_PATH, fields="all"
    )

    save_daps_table(cb_uk, "crunchbase_organisations", CB_PATH)
    save_daps_table(cb_funding_rounds_uk, "crunchbase_funding_rounds", CB_PATH)
    save_daps_table(cb_org_cats_uk, "crunchbase_organizations_categories", CB_PATH)
    save_daps_table(category_group, "crunchbase_category_groups", CB_PATH)


if __name__ == "__main__":
    fetch_save_crunchbase()