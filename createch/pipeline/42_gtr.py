# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: createch
#     language: python
#     name: createch
# ---

# ### Identify Createch projects

# +
# %load_ext autoreload
# %autoreload 2

import logging

import altair as alt
import pandas as pd
from createch import PROJECT_DIR
from createch.getters.gtr import (
    get_gtr_projects,
    get_organisations,
    get_project_orgs_lookup,
    get_gtr_topics,
    get_gtr_disciplines,
)
from createch.pipeline.topic_modelling import filter_topics
from createch.pipeline.network_analysis import make_network_from_coocc, make_topic_coocc
from createch.utils.altair_network import *
from createch.pipeline.utils import has_createch_sector
from createch.utils.altair_save_utils import *
from createch.utils.io import get_lookup
from cdlib import algorithms
from itertools import chain

# -

driv = google_chrome_driver_setup()


# +
# Functions


def has_community(comm_number, vector, comm_container, thres=0.04):
    """Checks if a project has topics from a community"""

    has_comm_number = any(vector[top] > thres for top in comm_container[comm_number])
    if has_comm_number is True:
        return True
    else:
        return False


def lq(table: pd.DataFrame) -> pd.DataFrame:
    """Calculate LQ for a category X in  population of categories Y
    Args:
        table where the rows are the categories X and columns the categories Y
    Returns:
        a table with LQs
    """

    denom = table.sum(axis=1) / table.sum().sum()

    return table.apply(lambda x: (x / x.sum()) / denom)


def make_topic_specialisation(topics_long, variable="discipline"):
    """Calculates a discipline's specialisation in a topic"""
    topic_wide = topics_long.pivot_table(
        index="variable", columns=variable, values="value"
    ).fillna(0)

    return lq(topic_wide)


def make_topic_distr(topic_mix, variable, threshold=0.1):
    """Calculates the topical activity in another variable"""

    long = (
        topic_mix.melt(id_vars=variable)
        .query(f"value>{threshold}")
        .groupby(["variable", variable])
        .size()
    )
    return long.reset_index(name="value")


def make_project_discipline_distribution(topics, disc_lookup, threshold):
    """ """
    topic_long = (
        topics.applymap(lambda x: x > threshold)
        .assign(discipline=lambda df: df.index.map(disc_lookup))
        .melt(id_vars="discipline", var_name="topic")
        .query("value>0")
        .groupby(["discipline", "topic"])["value"]
        .sum()
        .reset_index(drop=False)
    )

    return topic_long


# -

# ### Read data

# +
# Topic mix and discipline classification
topics = get_gtr_topics()
disciplines = get_gtr_disciplines()
projects = get_gtr_projects()


# Create lookups
project_title_lookup = projects.set_index("project_id")["title"].to_dict()
project_disc_lookup = disciplines.idxmax(axis=1).to_dict()
# -

orgs = get_organisations()

org_proj_lookup = get_project_orgs_lookup()

gtr_area_tagged = get_lookup("outputs/data/gtr/gtr_area_tagged")
project_date_lookup = {
    row["project_id"]: row["start"].year for _, row in projects.iterrows()
}

# ### Process data

# +
# Filter the topic mix and calculate discipline distributions and specialisations
topic_mix_filt, filtered_topics = filter_topics(
    topics, presence_thr=0.01, prevalence_thr=0.14
)

topic_discipline_distr = make_topic_distr(
    topic_mix_filt.assign(discipline=lambda df: df.index.map(project_disc_lookup)),
    variable="discipline",
    threshold=0.05,
)
topic_specialisation = make_topic_specialisation(topic_discipline_distr)

# Get topics in technology disciplines
sorted_topics = topic_specialisation.loc[
    (topic_specialisation["technology"] > 1.5)
    | (topic_specialisation["mathematics_computing_digital"] > 1.5)
].index.tolist()

topic_discipline_lookup = topic_specialisation.idxmax(axis=1).to_dict()

# +
# Make and visualise topic network

topic_net = make_network_from_coocc(
    make_topic_coocc(topic_mix_filt, 0.01), spanning=True, extra_links=200
)

pos = nx.kamada_kawai_layout(topic_net)
topic_degree_lookup = dict(topic_net.degree())

node_df = (
    pd.DataFrame(pos)
    .T.reset_index()
    .rename(columns={0: "x", 1: "y", "index": "node"})
    .assign(node_color=lambda df: df["node"].map(topic_discipline_lookup))
    .assign(node_name=lambda df: df["node"])
    .assign(node_size=lambda df: df["node"].map(topic_degree_lookup))
    # .dropna(axis=0, subset=['node_color'])
)

plot_altair_network(
    node_df,
    topic_net,
    node_color="node_color",
    node_label="node_name",
    node_size="node_size",
    show_neighbours=True,
    edge_opacity=0.08,
    node_size_title="Connections",
    edge_weight_title="project co-occurrences",
    title="Creative industries topic network",
    node_color_title="discipline",
).properties(width=700, height=400)

# -

# ### Industry distribution of topics

# +
# Topic by sector
# -

topic_mix_sector = (
    topic_mix_filt.reset_index(drop=False)
    .merge(org_proj_lookup, left_on="index", right_on="project_id")
    .merge(orgs[["creative_sector", "gtr_id"]], left_on="id", right_on="gtr_id")
    .drop(axis=1, labels=["gtr_id", "project_id", "id", "index"])
)

# +
topic_sector_distr = make_topic_distr(
    topic_mix_sector, variable="creative_sector", threshold=0.1
)

topic_sector_shares = (
    topic_sector_distr.groupby(["variable"])
    .apply(lambda df: df.groupby("creative_sector")["value"].sum() / df["value"].sum())
    .reset_index(drop=False)
    .assign(discipline=lambda df: df["variable"].map(topic_discipline_lookup))
    .sort_values("discipline")
    .reset_index(drop=True)
    .reset_index(drop=False)
)
# -

topic_sector_chart = (
    (
        alt.Chart(topic_sector_shares)
        .mark_bar(filled=True)
        .encode(
            y=alt.Y(
                "variable",
                sort=alt.EncodingSortField("index"),
                axis=alt.Axis(labels=False, ticks=False),
            ),
            facet=alt.Facet("discipline", columns=4),
            tooltip=["variable", "discipline", "creative_sector"],
            x="value",
            color=alt.Color("creative_sector", scale=alt.Scale(scheme="tableau20")),
        )
    )
    .properties(height=100, width=200)
    .resolve_scale(y="independent")
)
topic_sector_chart


# ### Identify tech topics and label createch

# +
# Focus on maths and technology sectors - build a co-occurrence network and find communities
# -

comms.communities

# +
technology_disciplines = [
    "mathematics_computing_digital",
    "technology",
    "energy_systems",
]

technology_topics = list(
    topic_specialisation.loc[
        [
            any(row[val] > 1.2 for val in technology_disciplines)
            for _, row in topic_specialisation.iterrows()
        ]
    ].index
)
# -

topic_net = make_network_from_coocc(
    (
        topic_mix_filt[technology_topics]
        .reset_index(drop=False)
        .melt(id_vars="index")
        .query("value>0.01")
        .reset_index(drop=False)
        .groupby("index")["variable"]
        .apply(lambda x: list(x))
    ),
    spanning=True,
    extra_links=200,
)

comms = algorithms.louvain(topic_net, resolution=0.4)
# [[n,c] for n,c in enumerate(comms.communities)]

# +
createch_topics = [2, 3, 8, 9]
createch_names = {
    2: "content_media",
    3: "ai_data",
    8: "consumers_products",
    9: "computing",
}

project_labels = pd.DataFrame(
    [
        [has_createch_sector(n, row, comms.communities) for n in createch_topics]
        for _, row in topic_mix_filt.iterrows()
    ],
    index=topic_mix_filt.index,
    columns=[createch_names[n] for n in createch_topics],
)
project_labels["blockchain"] = False

# +
# Merge with semantic identification
# -

tagged_segment_lookup = {
    "robotics": "to_drop",
    "simulation": "computing",
    "ai": "ai_data",
    "crypto": "blockchain",
    "immersive": "content_media",
    "visual_effects": "content_media",
    "web": "consumers_products",
    "3d_printing": "consumers_products",
}

# +
gtr_area_tagged_2 = {
    k: [tagged_segment_lookup[s] for s in v.keys()] for k, v in gtr_area_tagged.items()
}
project_labelled_semantic = (
    pd.Series(gtr_area_tagged_2.values(), index=gtr_area_tagged_2.keys())
    .reset_index(name="topics")
    .explode(column="topics")
    .dropna(axis=0, subset=["topics"])
    .assign(value=1)
    .pivot_table(index="index", columns="topics", values="value")
    .fillna(0)
    .drop(axis=1, labels=["to_drop"])
)

project_labelled_semantic_ci = project_labelled_semantic.loc[
    project_labelled_semantic.index.isin(project_labels.index)
]

project_labels_final = []

createch_labels = list(createch_names.values()) + ["blockchain"]

for _id in project_labels.index:
    consolidated_sectors = []
    for sector in createch_labels:
        if (_id in project_labels.index) & (_id in project_labelled_semantic_ci.index):
            if (project_labels.loc[_id, sector] == 0) & (
                project_labelled_semantic_ci.loc[_id, sector] == 0
            ):
                consolidated_sectors.append(0)
            else:
                consolidated_sectors.append(1)
        else:
            consolidated_sectors.append(project_labels.loc[_id, sector])
    project_labels_final.append(
        pd.Series(consolidated_sectors, index=createch_labels, name=_id)
    )


projects_createch_tags_df = pd.DataFrame(project_labels_final)

# -

projects_createch_tags_df["has_createch"] = projects_createch_tags_df.sum(axis=1) > 0
projects_createch_tags_df["year"] = projects_createch_tags_df.index.map(
    project_date_lookup
)

temp_distr = (
    projects_createch_tags_df.reset_index(drop=False)
    .melt(id_vars=["index", "year"])
    .query("value == True")
    .groupby("variable")["year"]
    .value_counts(normalize=False)
    .reset_index(name="year_share")
)
alt.Chart(temp_distr).mark_line().encode(
    x="year:O", y="year_share", color="variable"
).properties(width=500, height=300)


# +
# [(n,c) for n,c in enumerate(comms.communities)]
# -

# ### Save outputs

projects_createch_tags_df.to_csv(f"{PROJECT_DIR}/outputs/data/gtr_createch_tagged.csv")

createch_org_ids = set(
    org_proj_lookup.loc[
        org_proj_lookup["project_id"].isin(
            projects_createch_tags_df.query("has_createch==True").index
        )
    ]["id"]
)


orgs_ch = get_organisations(creative=False)

createch_orgs = (
    orgs_ch.reset_index(drop=True)
    .drop(axis=1, labels=["flag"])
    .rename(columns={"gtr_id": "org_id", "gtr_name": "org_name"})
    .assign(source="gtr")
    .assign(has_createch=lambda df: [_id in createch_org_ids for _id in df["org_id"]])
)

createch_orgs.to_csv(
    f"{PROJECT_DIR}/outputs/data/gtr/gtr_createch_orgs.csv", index=False
)

projects_createch_tags_df["has_createch"].sum()

# +
# createch_orgs['company_number'].to_csv(f"{PROJECT_DIR}/outputs/data/ch_number.csv")
# -

for k in projects_createch_tags_df.sample(10).index:
    print(project_title_lookup[k])
