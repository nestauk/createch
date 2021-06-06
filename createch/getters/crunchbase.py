import json
import logging
import os


import pandas as pd

from createch import PROJECT_DIR
from createch.getters.daps import fetch_daps_table, save_daps_table
from createch.getters.gtr import get_cis_lookup

CB_PATH = f"{PROJECT_DIR}/inputs/data/crunchbase"

if os.path.exists(CB_PATH) is False:
    os.makedirs(CB_PATH)


def get_crunchbase_orgs():
    return pd.read_csv(f"{CB_PATH}/crunchbase_organisations.csv")


def get_crunchbase_tokenised():
    with open(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_tokenised.json", "r"
    ) as infile:
        return json.load(infile)


def get_crunchbase_tagged():
    with open(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_area_tagged.json", "r"
    ) as infile:
        return json.load(infile)


def get_crunchbase_vocabulary():
    with open(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_vocabulary.json", "r"
    ) as infile:
        return json.load(infile)


def get_crunchbase_orgs_cats_uk():
    return pd.read_csv(
        f"{PROJECT_DIR}/inputs/data/crunchbase/crunchbase_organizations_categories.csv"
    )


def get_crunchbase_orgs_cats_all():

    return fetch_daps_table("crunchbase_organizations_categories")


def get_crunchbase_topics():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_topic_mix.csv", index_col=0
    )


def get_crunchbase_industry_pred():
    return pd.read_csv(
        f"{PROJECT_DIR}/outputs/data/crunchbase/predicted_industries.csv"
    )


def get_cb_ch_organisations(creative=True):

    SIC_IND_LOOKUP = get_cis_lookup()

    uk_orgs = set(get_crunchbase_orgs()["id"])

    cb_ch = pd.read_csv(
        f"{PROJECT_DIR}/inputs/data/crunchbase/crunchbase_ch_organisations.csv",
        dtype={"SIC4_code": str},
    )

    cb_ch = (
        cb_ch.loc[cb_ch["cb_id"].isin(uk_orgs)][
            [
                "cb_id",
                "sim_mean",
                "cb_name",
                "company_number",
                "ch_name",
                "SIC4_code",
                "ttwa_code",
                "ttwa_name",
            ]
        ]
        .drop_duplicates(subset=["cb_id"])
        .assign(creative_sector=lambda df: df["SIC4_code"].map(SIC_IND_LOOKUP))
    )

    if creative is True:
        cb_ch = cb_ch.dropna(axis=0, subset=["creative_sector"]).reset_index(drop=True)

    else:
        cb_ch = cb_ch.fillna(value={"creative_sector": "other"})

    return cb_ch


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
    cb_orgs = fetch_daps_table("crunchbase_organizations", fields="all")

    cb_uk = cb_orgs.loc[cb_orgs["country"] == "United Kingdom"].drop_duplicates(
        subset=["id"]
    )
    logging.info(len(cb_uk))
    save_daps_table(cb_uk, "crunchbase_organisations", CB_PATH)

    cb_uk_ids = set(cb_uk["id"])

    cb_funding_rounds = fetch_daps_table("crunchbase_funding_rounds", fields="all")
    cb_funding_rounds_uk = filter_uk(cb_funding_rounds, cb_uk_ids)

    cb_orgs_cats = fetch_daps_table("crunchbase_organizations_categories", fields="all")
    cb_org_cats_uk = filter_uk(cb_orgs_cats, cb_uk_ids, "organization_id")

    category_group = fetch_daps_table("crunchbase_category_groups", fields="all")

    save_daps_table(cb_funding_rounds_uk, "crunchbase_funding_rounds", CB_PATH)
    save_daps_table(cb_org_cats_uk, "crunchbase_organizations_categories", CB_PATH)
    save_daps_table(category_group, "crunchbase_category_groups", CB_PATH)


if __name__ == "__main__":
    fetch_save_crunchbase()
