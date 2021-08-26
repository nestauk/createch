# Script to extract CB organisations in the creative industries and train a
# topic model on them
import logging

from cdlib import algorithms

from createch import PROJECT_DIR
from createch.getters.crunchbase import (
    get_cb_ch_organisations,
    get_crunchbase_orgs_cats_all,
    get_crunchbase_orgs_cats_uk,
    get_crunchbase_tokenised,
)
from createch.getters.processing import save_model
from createch.pipeline.network_analysis import make_network_from_coocc
from createch.pipeline.topic_modelling import post_process_model, train_model

# from createch.utils.io import save_lookup


def make_creative_cat_tokenised(creative_comms, name_comm_lookup):
    """Get lookup of org - tokens for orgs in creative categories"""
    uk_org_cats = get_crunchbase_orgs_cats_uk().assign(
        comm_label=lambda df: df["category_name"].map(name_comm_lookup)
    )
    uk_org_cats_ci = uk_org_cats.loc[
        uk_org_cats["comm_label"].isin(creative_comms)
    ].reset_index(drop=True)

    uk_org_cats_id = set(uk_org_cats_ci["organization_id"])
    ci_tokenised = {
        k: v for k, v in get_crunchbase_tokenised().items() if k in uk_org_cats_id
    }
    return ci_tokenised


def make_creative_org_tokenised():
    """Lookup of orgs - tokens for orgs in creative sectors"""

    cb_ch_creative_ids = set(get_cb_ch_organisations()["cb_id"])
    ci_tokenised = {
        k: v for k, v in get_crunchbase_tokenised().items() if k in cb_ch_creative_ids
    }

    return ci_tokenised


def save_model_outputs(
    topsbm,
    topic_mix,
    # clusters,
    source,
):

    # doc_to_cluster_lookup = {doc[0]: k for k, v in clusters.items() for doc in v}

    save_model(topsbm, f"outputs/models/{source}/{source}_topsbm_creative")
    topic_mix.to_csv(f"{PROJECT_DIR}/outputs/data/{source}/{source}_topic_mix.csv")
    # save_lookup(doc_to_cluster_lookup, f"outputs/data/{source}/project_cluster_lookup")


def topic_model_creative():
    """Partitions crunchbase tags and identifies creative ones, trains topic model on creative firms."""
    logging.info("Making and partitioning crunchbase network")
    cat_coocc = (
        get_crunchbase_orgs_cats_all()
        .groupby("organization_id")["category_name"]
        .apply(lambda x: list(x))
    )

    cat_net = make_network_from_coocc(cat_coocc)
    partition = algorithms.louvain(cat_net, resolution=0.4)
    # comms_to_categories = {n: set(el) for n, el in enumerate(partition.communities)}

    creative_comms = [3, 5, 10, 14, 21]
    cats_to_comms_lookup = {
        el: n for n, val in enumerate(partition.communities) for el in val
    }

    logging.info("Labelling UK companies")
    ci_tokenised = make_creative_cat_tokenised(creative_comms, cats_to_comms_lookup)
    creative_org_tokenised = make_creative_org_tokenised()
    ci_tokenised_combined = {**ci_tokenised, **creative_org_tokenised}

    logging.info(len(ci_tokenised_combined))

    logging.info("Training topic model")
    cb_top_sbm = train_model(
        list(ci_tokenised_combined.values()), list(ci_tokenised_combined.keys())
    )

    # topic_mix_df = post_process_model_clusters(
    #     cb_top_sbm, top_level=0, cl_level=1
    # )
    topic_mix_df = post_process_model(cb_top_sbm, top_level=0)
    logging.info(topic_mix_df.head())

    save_model_outputs(cb_top_sbm, topic_mix_df, "crunchbase")


if __name__ == "__main__":
    topic_model_creative()
