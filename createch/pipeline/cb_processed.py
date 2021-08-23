# Creates a processed version of the CrunchBase data ready for analysis
import logging

import numpy as np
import pandas as pd

from createch import PROJECT_DIR
from createch.getters.crunchbase import (
    get_cb_ch_organisations,
    get_cb_ttwa,
    get_crunchbase_industry_pred,
    get_crunchbase_orgs,
    get_crunchbase_topics,
)
from createch.pipeline.topic_modelling import filter_topics
from createch.pipeline.utils import has_createch_sector
from createch.utils.io import get_lookup


def make_org_description_lu(orgs):

    descr = {
        r["id"]: r["long_description"]
        if pd.isnull(r["long_description"]) == False
        else r["short_description"]
        for _, r in orgs.iterrows()
    }
    return descr


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

if __name__ == "__main__":
    logging.info("Reading data")

    cb_topics, filtered_topics = filter_topics(
        get_crunchbase_topics(), presence_thr=0.1, prevalence_thr=0.1
    )
    orgs = get_crunchbase_orgs()
    org_descr_lookup = make_org_description_lu(orgs)
    cb_area_tagged = get_lookup("outputs/data/crunchbase/crunchbase_area_tagged")
    comp_industry = get_crunchbase_industry_pred()

    cb_ch_orgs = get_cb_ch_organisations(creative=False)
    cb_ttwa = get_cb_ttwa().set_index("id").to_dict(orient="dict")
    cb_metadata = orgs.set_index("id")[
        ["name", "founded_on", "employee_count"]
    ].to_dict(orient="dict")

    logging.info("label createch based on topics")
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

    logging.info("label createch based on keywords")

    cb_area_tagged_2 = {
        k: [tagged_segment_lookup[s] for s in v.keys()]
        for k, v in cb_area_tagged.items()
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

    comp_labelled_semantic_ci = cb_comp_labelled_semantic.loc[
        cb_comp_labelled_semantic.index.isin(org_labels.index)
    ]

    org_labels_expanded = []

    logging.info("Consolidating labels")

    createch_labels = createch_names.copy()

    for _id in orgs["id"]:
        consolidated_sectors = []
        for sector in createch_labels:
            if (_id in org_labels.index) & (_id in comp_labelled_semantic_ci.index):
                if (org_labels.loc[_id, sector] == 0) & (
                    comp_labelled_semantic_ci.loc[_id, sector] == 0
                ):
                    consolidated_sectors.append(0)
                else:
                    consolidated_sectors.append(1)
            elif _id in org_labels.index:
                consolidated_sectors.append(org_labels.loc[_id, sector])
            else:
                consolidated_sectors.append(0)
        org_labels_expanded.append(
            pd.Series(consolidated_sectors, index=createch_labels, name=_id)
        )

    org_labels_expanded_df = (
        pd.DataFrame(org_labels_expanded)
        .assign(no_createch=lambda df: (df.sum(axis=1) == 0).astype(int))
        .stack()
        .reset_index(level=1)
        .rename(columns={"level_1": "createch_sector", 0: "value"})
    )

    logging.info("Adding extra metadata")

    org_labels_expanded_df = (
        org_labels_expanded_df.assign(
            tech=lambda df: df.index.map(
                comp_industry.set_index("id")["IT software and computer services"]
                > 0.15
            )
        )
        .assign(
            sic_sector=lambda df: df.index.map(
                cb_ch_orgs.set_index("cb_id")["creative_sector"]
            )
        )
        .assign(ttwa_name=lambda df: df.index.map(cb_ttwa["ttwa11nm"]))
        .assign(ttwa_code=lambda df: df.index.map(cb_ttwa["ttwa11cd"]))
    )

    for var in cb_metadata.keys():
        org_labels_expanded_df[var] = org_labels_expanded_df.index.map(cb_metadata[var])

    org_labels_expanded_df["year"] = [
        int(x.split("-")[0]) if pd.isnull(x) == False else np.nan
        for x in org_labels_expanded_df["founded_on"]
    ]
    logging.info("Saving data")
    org_labels_expanded_df.to_csv(f"{PROJECT_DIR}/inputs/data/cb_processed.csv")
