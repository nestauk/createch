import json
import logging
import os
from functools import lru_cache
from typing import Dict

import geopandas as gp
from numpy import index_exp
import pandas as pd
from metaflow import namespace, Run

import createch
from createch import PROJECT_DIR
from createch.getters.daps import fetch_daps_table, save_daps_table
from createch.getters.gtr import get_cis_lookup
from createch.pipeline.fetch_daps1_data.cb_utils import CB_PATH

logger = logging.getLogger(__name__)
namespace(None)

RUN_ID: int = createch.config["flows"]["nesta"]["run_id"]


def get_crunchbase_orgs():
    return (
        pd.read_csv(f"{CB_PATH}/crunchbase_organisations.csv")
        .drop_duplicates("id")
        .reset_index(drop=True)
    )


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


def make_long_description(cb):

    cb_ = cb.copy().dropna(axis=0, subset=["short_description"])
    cb_["descr_combined"] = [
        row["long_description"]
        if pd.isnull(row["long_description"]) == False
        else row["short_description"]
        for _id, row in cb_.iterrows()
    ]
    return cb_.set_index("id")["descr_combined"].to_dict()


def get_crunchbase_description():

    return make_long_description(get_crunchbase_orgs())


def get_crunchbase_processed():
    return pd.read_csv(f"{PROJECT_DIR}/inputs/data/crunchbase/cb_processed.csv")


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


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CreatechNestaGetter/{run_id}")


def get_name() -> Dict[str, str]:
    """Lookup between Crunchbase organisation ID and name."""
    return _flow(RUN_ID).data.crunchbase_names


def make_cb_ttwa():
    """Makes the CB - TTWA lookup"""
    geo_cb = fetch_daps_table("geographic_data")
    orgs = get_crunchbase_orgs().drop_duplicates("id").reset_index(drop=True)

    orgs_geo_coords = orgs[["id", "name", "city", "location_id"]].merge(
        geo_cb[["id", "latitude", "longitude"]],
        left_on="location_id",
        right_on="id",
    )

    ttwa_boundaries = gp.read_file(
        f"{PROJECT_DIR}/inputs/data/ttwa_shapefile/Travel_to_Work_Areas__December_2011__Super_Generalised_Clipped_Boundaries_in_United_Kingdom.shp"
    )
    ttwa_boundaries = ttwa_boundaries.to_crs(epsg=4326)

    orgs_geo_p = gp.GeoDataFrame(
        orgs_geo_coords,
        geometry=gp.points_from_xy(orgs_geo_coords.longitude, orgs_geo_coords.latitude),
    )
    orgs_geo_p = orgs_geo_p.set_crs(epsg=4326)

    orgs_ttwa = gp.sjoin(orgs_geo_p, ttwa_boundaries, op="within")[
        ["id_x", "name", "city", "ttwa11cd", "ttwa11nm"]
    ].rename(columns={"id_x": "id"})

    return orgs_ttwa


def get_cb_ttwa(overwrite=False):
    """Reads a CB_TTWA lookup"""

    cb_ttwa_path = f"{PROJECT_DIR}/inputs/data/cb_ttwa_lookup.csv"
    if os.path.exists(cb_ttwa_path) is False:
        logging.info("Creating cb - ttwa lookup")
        make_cb_ttwa().to_csv(cb_ttwa_path, index=False)
        return pd.read_csv(cb_ttwa_path)

    else:
        if overwrite is True:
            make_cb_ttwa().to_csv(cb_ttwa_path, index=False)
            return pd.read_csv(cb_ttwa_path)
        else:
            return pd.read_csv(cb_ttwa_path)
