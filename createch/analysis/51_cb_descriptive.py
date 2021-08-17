# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import altair as alt
import pandas as pd
from createch import PROJECT_DIR
from createch.getters.crunchbase import (
    get_crunchbase_topics,
    get_crunchbase_orgs,
    get_crunchbase_industry_pred,
    get_cb_ch_organisations,
)
from createch.pipeline.topic_modelling import filter_topics
from createch.pipeline.network_analysis import make_network_from_coocc, make_topic_coocc
from createch.utils.altair_network import *
from createch.pipeline.utils import has_createch_sector
from createch.utils.altair_save_utils import *
from createch.utils.io import get_lookup
from cdlib import algorithms
from itertools import chain
from numpy.random import choice


# %%
def make_org_description_lu(orgs):

    descr = {
        r["id"]: r["long_description"]
        if pd.isnull(r["long_description"]) == False
        else r["short_description"]
        for _, r in orgs.iterrows()
    }
    return descr


# %%
# Read data
cb_topics, filtered_topics = filter_topics(
    get_crunchbase_topics(), presence_thr=0.1, prevalence_thr=0.1
)
orgs = get_crunchbase_orgs()
org_descr_lookup = make_org_description_lu(orgs)

cb_area_tagged = get_lookup("outputs/data/crunchbase/crunchbase_area_tagged")

comp_industry = get_crunchbase_industry_pred()
cb_ch_orgs = get_cb_ch_organisations(creative=False)

createch_names = [
    "content_media",
    "ai_data",
    "consumers_products",
    "computing",
    "blockchain",
]
createch_areas = [0, 1, 2, 3, 4]
createch_topics = [
    [
        "vr_virtual_reality_immersive_reality_augmented_reality",
        "production_entertainment_interactive_distribution_channel",
        "tv_streaming_broadcasters_bbc_sky",
        "film_video_production_films_production_company_television",
        "content_video_videos_format_stream",
        "music_artists_fans_musicians_artist",
        "studio_studios_animation_motion_storytelling",
    ],
    [
        "data_analytics_insights_intelligence_data_driven",
        "ai_artificial_intelligence_machine_learning_decisions_big_data",
    ],
    [
        "seo_search_engine_optimisation_email_marketing_optimization",
        "publishers_advertisers_ad_ads_inventory",
        "agency_digital_marketing_marketing_agency_digital_agency_advertising_agency",
        "marketing_social_media_lead_generation_content_creation_marketing_strategy",
        "apps_developers_developer_mobile_apps_code",
    ],
    [
        "monitoring_iot_safety_transport_internet_things",
        "home_smart_room_spaces_homes",
        "software_systems_applications_engineering_computer",
        "users_app_user_location_mobile_app",
    ],
    [
        "exchange_blockchain_asset_crypto_bitcoin",
    ],
]

org_labels = pd.DataFrame(
    [
        [
            has_createch_sector(n, row, createch_topics, thres=0.1)
            for n in createch_areas
        ]
        for _, row in cb_topics.iterrows()
    ],
    index=cb_topics.index,
    columns=createch_names,
)

# +
# Add semantic tagging
# -

tagged_segment_lookup = {
    "robotics": "to_drop",
    "simulation": "computing",
    "ai": "ai_data",
    "crypto": "blockchain",
    "immersive": "content_media",
    "visual_effects": "content_media",
    "web": "consumers_products",
    "3d_printing": "services",
}

cb_area_tagged_2 = {
    k: [tagged_segment_lookup[s] for s in v.keys()] for k, v in cb_area_tagged.items()
}
cb_comp_labelled_semantic = (
    pd.Series(cb_area_tagged_2.values(), index=cb_area_tagged_2.keys())
    .reset_index(name="topics")
    .explode(column="topics")
    .dropna(axis=0, subset=["topics"])
    .assign(value=1)
    .pivot_table(index="index", columns="topics", values="value")
    .fillna(0)
    .drop(axis=1, labels=["to_drop"])
)

# +
# Here we are looping over the CI based and semantic expansion dfs and assigning
# a 1 to a createch area if it appears in either

comp_labelled_semantic_ci = cb_comp_labelled_semantic.loc[
    cb_comp_labelled_semantic.index.isin(org_labels.index)
]

org_labels_expanded = []

createch_labels = createch_names.copy()

for _id in org_labels.index:
    consolidated_sectors = []
    for sector in createch_labels:
        if (_id in org_labels.index) & (_id in comp_labelled_semantic_ci.index):
            if (org_labels.loc[_id, sector] == 0) & (
                comp_labelled_semantic_ci.loc[_id, sector] == 0
            ):
                consolidated_sectors.append(0)
            else:
                consolidated_sectors.append(1)
        else:
            consolidated_sectors.append(org_labels.loc[_id, sector])
    org_labels_expanded.append(
        pd.Series(consolidated_sectors, index=createch_labels, name=_id)
    )


org_labels_expanded_df = pd.DataFrame(org_labels_expanded)
# -

org_labels_expanded_df.sum()

# +
high_tech = set(
    comp_industry.loc[comp_industry["IT software and computer services"] > 0.15]["id"]
)

org_labels_expanded_df["is_createch"] = org_labels_expanded_df.sum(axis=1) > 0
org_labels_expanded_df = org_labels_expanded_df.query("is_createch==True")

org_labels_expanded_df_tech = org_labels_expanded_df.loc[
    org_labels_expanded_df.index.isin(high_tech)
]

# %%
# +
# for t in createch_names:
#     logging.info(t)
#     orgs_rel = choice(org_labels_expanded_df_tech.loc[org_labels_expanded_df_tech[t]==True].index,5)

#     for o in orgs_rel:
#         print(org_descr_lookup[o][:300])
#         print("\n")

# -

cb_createch_orgs = (
    cb_ch_orgs.loc[cb_ch_orgs["cb_id"].isin(org_labels_expanded_df_tech.index)]
    .rename(columns={"cb_id": "org_id", "cb_name": "org_name"})
    .assign(source="crunchbase")
    .reset_index(drop=True)
)
cb_createch_orgs.to_csv(
    f"{PROJECT_DIR}/outputs/data/crunchbase/cb_createch_orgs.csv", index=False
)

org_labels_expanded_df_tech.to_csv(
    f"{PROJECT_DIR}/outputs/data/crunchbase/cb_createch_tagged.csv"
)

org_labels_expanded_df_tech.sum()

ch_number = set(
    pd.read_csv(f"{PROJECT_DIR}/outputs/data/ch_number.csv")["company_number"]
)

len(ch_number.union(set(cb_createch_orgs["company_number"])))
