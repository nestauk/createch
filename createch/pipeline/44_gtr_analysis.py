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

# # GtR analysis
#
#

# ## 0. Preamble

# +
# %load_ext autoreload
# %autoreload 2

import logging

import re
import altair as alt
import numpy as np
from numpy.random import choice
import pandas as pd
from createch import PROJECT_DIR
from createch.getters.gtr import (
    get_gtr_projects,
    get_gtr_createch_tagged,
    get_gtr_orgs_tagged,
    get_organisations,
    get_project_orgs_lookup,
    get_gtr_topics,
    get_gtr_disciplines,
)
from createch.pipeline.topic_modelling import filter_topics
from createch.utils.sic_utils import (
    section_code_lookup,
    extract_sic_code_description,
    load_sic_taxonomy,
)
from createch.pipeline.network_analysis import make_network_from_coocc, make_topic_coocc
from createch.utils.altair_network import *
from createch.pipeline.utils import has_createch_sector
from createch.utils.altair_save_utils import *
from createch.utils.io import get_lookup
from cdlib import algorithms
from itertools import chain

# -

FIG_PATH = f"{PROJECT_DIR}/docs/report1_figures"
os.makedirs(FIG_PATH + "/png", exist_ok=True)
os.makedirs(FIG_PATH + "/html", exist_ok=True)
os.makedirs(FIG_PATH + "/tables", exist_ok=True)

driv = google_chrome_driver_setup()


def change_font_sizes(alt_chart):
    ch = alt_chart.configure_axis(labelFontSize=14, titleFontSize=16).configure_legend(
        labelFontSize=14, titleFontSize=16
    )
    return ch


# ## 1. Reading

projects = get_gtr_projects()
disciplines = get_gtr_disciplines()

orgs_projects_link = get_project_orgs_lookup()

projects_createch = get_gtr_createch_tagged()
orgs_createch = get_gtr_orgs_tagged()

# +
div_section_lookup = section_code_lookup()
section_names = {
    k: k + ": " + v
    for k, v in extract_sic_code_description(load_sic_taxonomy(), "SECTION").items()
}


def make_sic4_section_name(code):

    return section_names[div_section_lookup[code[:2]]]


# -

# ## 2. Analysing

# +
# Number of createch projects

createch_labels = [
    "content_media",
    "ai_data",
    "consumers_products",
    "computing",
    "blockchain",
]
createch_clean_labels = [
    "Content and Media",
    "Data and AI",
    "Creative services",
    "Computing",
    "Blockchain",
]
createch_name_lookup = {n: l for n, l in zip(createch_labels, createch_clean_labels)}
createch_name_lookup["has_createch"] = "All Createch"
# -

# ### a. Trends

project_trends = (
    projects_createch.reset_index(drop=False)
    .query("has_createch==True")
    .melt(id_vars=["index", "year"])
    .groupby(["year", "variable"])["value"]
    .sum()
    .reset_index(drop=False)
    .assign(Category=lambda df: df["variable"].map(createch_name_lookup))
)

# +
trend_chart = (
    alt.Chart(project_trends.loc[project_trends["year"].isin(range(2006, 2021))])
    .mark_line()
    .encode(
        x=alt.X(
            "year:O",
            # scale=alt.Scale(domain=np.arange(2006,2021)),
            title=None,
        ),
        y=alt.Y("value", title="Number of projects"),
        color="Category",
    )
).properties(width=500, height=300)
trend_chart

save_altair(change_font_sizes(trend_chart), "all_trends", driver=driv, path=FIG_PATH)

trend_chart


# -


def get_createch_share_of_total(y):

    ct_ = int(
        project_trends.query(f"year=={y}").query("Category=='All Createch'")["value"]
    )
    sh = len(projects.loc[[x.year == y for x in projects["start"]]])

    return ct_ / sh


100 * get_createch_share_of_total(2020)

get_createch_share_of_total(2020) / get_createch_share_of_total(2019)

# ### b. Sectors

# +
# projects_to_orgs.groupby(['project_id'])['org_id'].apply(lambda x: len(list(x)))

# +
projects_to_orgs = (
    projects_createch.reset_index(drop=False)
    .melt(id_vars=["index", "year"])
    .query("value==1")
    .merge(orgs_projects_link, left_on="index", right_on="project_id")
    .merge(orgs_createch, left_on="id", right_on="org_id", how="inner")
)

projects_sectors = (
    projects_to_orgs.query("variable!='has_createch'")
    .groupby(["creative_sector"])["variable"]
    .value_counts(normalize=False)
    .reset_index(name="projects")
    .assign(Category=lambda df: df["variable"].map(createch_name_lookup))
)
sector_bar = (
    alt.Chart(projects_sectors.query("creative_sector!='Crafts'"))
    .mark_bar()
    .encode(
        y=alt.Y(
            "creative_sector",
            title=None,
            sort=alt.EncodingSortField("projects", "sum", order="descending"),
        ),
        x=alt.X("projects", title="Number of project participations"),
        color="Category",
    )
).properties(width=450, height=250)


save_altair(change_font_sizes(sector_bar), "all_sector", driver=driv, path=FIG_PATH)
# -

sector_bar

# +
sector_totals = projects_sectors.groupby("creative_sector")["projects"].sum()

sector_totals.loc[sector_totals.index != "Other"] / sector_totals.loc[
    sector_totals.index != "Other"
].sum()


cat_totals = projects_sectors.groupby(["creative_sector", "Category"]).apply(
    lambda x: x["projects"].sum()
)
cat_totals["Other"] / cat_totals["Other"].sum()

# +
# Trends

sector_trends = (
    projects_to_orgs.loc[projects_to_orgs["year"].isin(range(2006, 2021))]
    .groupby(["year", "creative_sector", "variable"])
    .size()
    .reset_index(name="projects")
    .query("creative_sector!='Crafts'")
    .assign(Category=lambda df: df["variable"].map(createch_name_lookup))
)

# +
sector_trend_chart = (
    (
        alt.Chart(sector_trends)
        .transform_window(
            mean="mean(projects)", frame=[-2], groupby=["creative_sector", "variable"]
        )
        .mark_line()
        .encode(
            x=alt.X("year:O", title=None),
            y=alt.Y("mean:Q", title=["Project", "participations"]),
            facet=alt.Facet(
                "creative_sector",
                title="Sector",
                columns=3,
                sort=alt.EncodingSortField("projects", "sum", order="descending"),
            ),
            color="Category",
        )
    )
    .resolve_scale(y="independent")
    .properties(width=200, height=100)
)

save_altair(
    change_font_sizes(sector_trend_chart), "all_sectors", driver=driv, path=FIG_PATH
)
sector_trend_chart

# +
# What do companies in non creative sectors do?

other_sectors = (
    projects_to_orgs.query("has_createch==True")
    .query("variable!='has_createch'")
    .query("creative_sector=='Other'")
    .assign(sic_section=lambda df: df["SIC4_code"].apply(make_sic4_section_name))
    .assign(Category=lambda df: df["variable"].map(createch_name_lookup))
    .groupby("sic_section")["Category"]
    .value_counts()
    .reset_index(name="projects")
)
sector_distr = (
    alt.Chart(other_sectors)
    .mark_bar()
    .encode(
        y=alt.Y("sic_section", title="SIC Section"),
        x=alt.X("projects", title="Number of project participations"),
        color=alt.Color("Category"),
    )
).properties(width=500, height=350)

save_altair(
    change_font_sizes(sector_distr), "other_sectors", driver=driv, path=FIG_PATH
)
sector_distr
# -

# ### c. Examples

section_names

# +
project_lookup = projects.set_index("project_id").to_dict(orient="index")


def fix_names(name):

    cap = re.sub("Bbc", "BBC", " ".join([x.capitalize() for x in name.split(" ")]))
    return cap


# +
example_table = []


for n in createch_labels:

    rel_projects = list(projects_createch.loc[projects_createch[n] > 0].index)

    titles = []

    for _i in choice(rel_projects, 3):
        titles.append(project_lookup[_i]["title"])

    rel_orgs = projects_to_orgs.loc[
        projects_to_orgs["project_id"].isin(rel_projects)
    ].query("org_name!='unknown'")
    creative_orgs = ", ".join(
        [
            fix_names(x)
            for x in (
                rel_orgs.query("creative_sector!='Other'")["org_name"]
                .value_counts()
                .index[:5]
            )
        ]
    )
    non_creative_orgs = ", ".join(
        [
            fix_names(x)
            for x in (
                rel_orgs.query("creative_sector=='Other'")["org_name"]
                .value_counts()
                .index[:5]
            )
        ]
    )

    example_table.append([", ".join(titles), creative_orgs, non_creative_orgs])


example_cols = [
    "Example projects",
    "Top organisations in creative SIC",
    "Top organisations outside creative SIC",
]
example_table_df = pd.DataFrame(
    example_table, index=createch_labels, columns=example_cols
)
example_table_df.index = example_table_df.index.map(createch_name_lookup)
example_table_df.index.name = "Createch category"

example_table_df.to_csv(f"{FIG_PATH}/tables/examples.csv")

example_table_df.to_markdown(f"{FIG_PATH}/tables/examples.md")


# -

# ### d. Geography
#
# Including comparison with creative industries more broadly

from createch.pipeline.geo import *

shape = read_shape()

# +
projects_to_orgs_all = (
    projects_createch.reset_index(drop=False)
    .melt(id_vars=["index", "year"])
    .merge(orgs_projects_link, left_on="index", right_on="project_id")
    .merge(orgs_createch, left_on="id", right_on="org_id", how="inner")
)

project_createch_share = (
    projects_to_orgs_all.groupby("has_createch")["ttwa_name"]
    .value_counts(normalize=True)
    .reset_index(name="share")
)
# -

createch, non_createch = [
    plot_counts(
        shape,
        project_createch_share.query(f"has_createch=={var}"),
        "share",
        name,
        scale_type="log",
    ).properties(width=300, height=500)
    for var, name in zip([True, False], ["Createch", "Other creative"])
]

maps = alt.hconcat(createch, non_createch)

# +
top_30_ttwas = (
    project_createch_share.query("has_createch==True")
    .sort_values("share", ascending=False)["ttwa_name"][:30]
    .tolist()
)

geo_share_table = project_createch_share.loc[
    project_createch_share["ttwa_name"].isin(top_30_ttwas)
].assign(
    ct_label=lambda df: [
        "Createch" if x == True else "Other Creative" for x in df["has_createch"]
    ]
)

geo_share_base = alt.Chart(geo_share_table).encode(
    y=alt.Y(
        "ttwa_name",
        sort=top_30_ttwas,
        title=None,
        # axis=alt.Axis(labelAngle=310)
    ),
    x=alt.X(
        "share",
        scale=alt.Scale(type="log"),
        title="National share",
        axis=alt.Axis(format="%"),
    ),
)


geo_share_points = geo_share_base.mark_point(
    filled=True, stroke="black", strokeWidth=1
).encode(color=alt.Color("ct_label", title="Category"))
geo_share_lines = geo_share_base.mark_line(strokeWidth=2, strokeDash=[1, 1]).encode(
    detail="ttwa_name"
)

geo_shares_chart = (geo_share_points + geo_share_lines).properties(
    width=300, height=500
)
geo_shares_chart
# -

maps_shares = change_font_sizes(
    alt.hconcat(maps, geo_shares_chart).configure_view(strokeWidth=0)
)

save_altair(maps_shares, "maps_shares", driver=driv, path=FIG_PATH)

maps_shares

project_createch_share.pivot_table(index="ttwa_name", columns="has_createch").fillna(
    0
).corr(method="spearman")

# +
project_ttwa_shares = project_createch_share.pivot_table(
    index="ttwa_name", columns="has_createch", values="share"
).fillna(0)


def get_concentration(value, index):

    return project_ttwa_shares.sort_values(value, ascending=False)[value].cumsum()[
        index
    ]


for v in True, False:
    print(get_concentration(v, 0))
# -

35.6 / 26.6

# +
# Shares by createch segment

geo_shares_segment = (
    projects_to_orgs_all.query("value==1")
    .groupby("variable")["ttwa_name"]
    .value_counts(normalize=True)
    .reset_index(name="share")
)
geo_shares_segment_top = (
    geo_shares_segment.loc[geo_shares_segment["ttwa_name"].isin(top_30_ttwas)]
    .reset_index(drop=True)
    .assign(Category=lambda df: df["variable"].map(createch_name_lookup))
)

# +
geo_segment_base = alt.Chart(geo_shares_segment_top).encode(
    y=alt.Y("ttwa_name", sort=top_30_ttwas),
    x=alt.X("share", axis=alt.Axis(format="%"), scale=alt.Scale(type="log")),
)
geo_segment_points = geo_segment_base.mark_point(
    filled=True, stroke="black", strokeWidth=1, size=50
).encode(color="Category", tooltip=["ttwa_name", "Category", "share"])

geo_segment_lines = geo_segment_base.mark_line(strokeWidth=1, strokeDash=[1, 1]).encode(
    detail="ttwa_name"
)

(geo_segment_points + geo_segment_lines).properties(width=600, height=350)
# -


# ### e. Interdisciplinarity

alt.data_transformers.disable_max_rows()

# +
from scipy.stats import entropy

project_entropy = disciplines.apply(lambda x: entropy(x / x.sum()), axis=1)

# +
median_project_description = np.median(
    [len(x) for x in projects["abstractText"].dropna()]
)

verbose_project_ids = (
    projects.dropna(axis=0, subset=["abstractText"])
    .assign(
        verbose=lambda df: [
            len(x) > median_project_description for x in df["abstractText"]
        ]
    )
    .query("verbose == True")["project_id"]
    .tolist()
)

# +
projects_createch_verb = projects_createch.loc[
    projects_createch.index.isin(verbose_project_ids)
]

projects_createch_verb["entropy"] = projects_createch_verb.index.map(project_entropy)

disc_entropy = projects_createch_verb["entropy"].dropna()

entropy_bins = np.linspace(min(disc_entropy), max(disc_entropy), 50)

entropy_createch_df = pd.concat(
    [
        pd.cut(
            projects_createch_verb.query(f"has_createch=={value}")["entropy"],
            entropy_bins,
        ).value_counts(normalize=True)
        for value in [False, True]
    ],
    axis=1,
)
entropy_createch_df.columns = ["Other creative", "Createch"]

entropy_createch_df_long = (
    entropy_createch_df.reset_index(drop=False)
    .melt(id_vars="index")
    .assign(mid=lambda df: [i.mid for i in df["index"]])
    .drop(axis=1, labels="index")
)
# -

createc_entropy_hist = (
    alt.Chart(entropy_createch_df_long)
    .transform_window(mean="mean(value)", frame=[-10, 10], groupby=["variable"])
    .mark_line(point=True)
    .encode(
        x=alt.X("mid", title="Disciplinary mixing (Shannon entropy)"),
        y=alt.Y("mean:Q", axis=alt.Axis(format="%"), title="Share of projects"),
        color=alt.Color("variable", title="Category"),
    )
    .properties(width=500, height=250)
)
save_altair(
    change_font_sizes(createc_entropy_hist), "entropy_distr", driver=driv, path=FIG_PATH
)

# +
# Entropy by createch segment

entropy_bins = np.linspace(min(disc_entropy), max(disc_entropy), 75)

entropy_createch_cats_df = pd.concat(
    [
        pd.cut(
            projects_createch_verb.loc[projects_createch[c] == True]["entropy"],
            entropy_bins,
        ).value_counts(normalize=True)
        for c in createch_labels
    ],
    axis=1,
)
entropy_createch_cats_df.columns = createch_labels

entropy_cats_df_long = (
    entropy_createch_cats_df.reset_index(drop=False)
    .melt(id_vars="index")
    .assign(mid=lambda df: [i.mid for i in df["index"]])
    .drop(axis=1, labels="index")
    .assign(Category=lambda df: df["variable"].map(createch_name_lookup))
)

createch_cats_entropy_hist = (
    alt.Chart(entropy_cats_df_long)
    .transform_window(mean="mean(value)", frame=[-20, 20], groupby=["Category"])
    .mark_line(point=True)
    .encode(
        x=alt.X("mid", title="Shannon Entropy"),
        y=alt.Y("mean:Q", axis=alt.Axis(format="%"), title="Share of projects"),
        color=alt.Color("Category"),
    )
    .properties(width=500, height=250)
)

createch_cats_entropy_hist

# -

entropies = change_font_sizes(
    alt.vconcat(createc_entropy_hist, createch_cats_entropy_hist).resolve_scale(
        y="independent", color="independent"
    )
)

save_altair(entropies, "entropy_charts", driver=driv, path=FIG_PATH)

createch_project_ids = projects_createch.query("has_createch==True").index

# +
examples_high_entropy = []

entropy_quantised = pd.qcut(
    project_entropy.loc[
        (project_entropy.index.isin(createch_project_ids))
        & (project_entropy.index.isin(verbose_project_ids))
    ],
    q=5,
    labels=False,
)
for c in createch_labels:

    high_entropy_projs = entropy_quantised.loc[entropy_quantised == 4].index.tolist()
    projects_in_cat = projects_createch_verb.loc[
        projects_createch_verb[c] == True
    ].index.tolist()
    relevant_ids = list(set(high_entropy_projs) & set(projects_in_cat))

    titles = []
    for p in choice(relevant_ids, 5, replace=False):

        titles.append(project_lookup[p]["title"])

    examples_high_entropy.append("\n ".join(titles))

examples_high_entropy_df = pd.DataFrame(
    examples_high_entropy,
    index=[createch_name_lookup[x] for x in createch_labels],
    columns=["High entropy examples"],
)

examples_high_entropy_df.to_csv(f"{FIG_PATH}/tables/high_entropy.csv")
# -

# ### f. Networks

# +
import networkx as nx
from createch.pipeline.network_analysis import make_network_from_coocc
from createch.utils.altair_network import plot_altair_network
from createch.getters.daps import fetch_daps_table
from cdlib import algorithms
from createch import config

ed_terms = config["gtr_organisations"]["filter_terms"]


# -


def get_uk_orgs():
    # Read all orgs and get their names
    all_gtr_orgs = fetch_daps_table("gtr_organisations")

    gtr_addresses = fetch_daps_table("gtr_organisations_locations")
    uk_orgs = set(
        gtr_addresses.loc[gtr_addresses["country_name"] == "United Kingdom"]["id"]
    )
    gtr_id_name_lookup = (
        all_gtr_orgs.loc[all_gtr_orgs["id"].isin(uk_orgs)]
        .set_index("id")["name"]
        .to_dict()
    )
    return gtr_id_name_lookup


# +
# Steps

# Read all orgs and get their names
uk_orgs = get_uk_orgs()

# +
# Find createch projects in the link_table
createch_project_id = set(
    projects_createch
    #                     .query("has_createch==True")
    .index
)

org_projects_link_createch = orgs_projects_link.loc[
    orgs_projects_link["project_id"].isin(createch_project_id)
]
# Name orgs with their names
org_proj_link_createch = (
    org_projects_link_createch.loc[org_projects_link_createch["id"].isin(uk_orgs)]
    .assign(name=lambda df: df["id"].map(uk_orgs))
    .query("name!='Unknown'")
)

# Additional labels
orgs_ch_id = set(orgs_createch["org_id"])

org_proj_link_createch["segment"] = [
    "Industry"
    if r["id"] in orgs_ch_id
    else "Academic"
    if any(t in r["name"].lower() for t in ed_terms)
    else "Other"
    for _id, r in org_proj_link_createch.iterrows()
]

org_segment_lookup = (
    org_proj_link_createch.drop_duplicates("id").set_index("name")["segment"].to_dict()
)


# +
def make_cat_co_occ(org_project_lookup, projects_createch, category, thres=0):
    # filter by category
    cat_ids = set(projects_createch.loc[projects_createch[category] == True].index)
    cat_project_links = org_project_lookup.loc[
        org_project_lookup["project_id"].isin(cat_ids)
    ]

    org_participations = cat_project_links.groupby("name").size()
    # Filter to focus in active organisations (if necessary)
    active_organisations = org_participations.loc[
        org_participations > np.quantile(org_participations, thres)
    ].index

    org_projects = cat_project_links.loc[
        cat_project_links["name"].isin(active_organisations)
    ]

    # Build network
    co_occ = org_projects.groupby("project_id")["name"].apply(lambda x: list(x))
    return co_occ, org_participations


def make_category_network(
    org_project_lookup,
    projects_createch,
    org_segment_lookup,
    category="ai_data",
    thres=0,
    visualise=True,
):

    co_occ, org_participations = make_cat_co_occ(
        org_project_lookup, projects_createch, category=category, thres=thres
    )
    net = make_network_from_coocc(co_occ, spanning=True, extra_links=0)

    if visualise is True:

        # Visualise
        # Find largest connected component
        largest_cc = max(nx.connected_components(net), key=len)
        net_conn = net.subgraph(largest_cc)

        comm = algorithms.louvain(net_conn)
        comm_net_lookup = {
            el: str(n) for n, comm in enumerate(comm.communities) for el in comm
        }

        pos = nx.kamada_kawai_layout(net_conn)

        node_df = (
            pd.DataFrame(pos)
            .T.reset_index()
            .rename(columns={0: "x", 1: "y", "index": "node"})
            .assign(node_color=lambda df: df["node"].map(comm_net_lookup))
            .assign(node_shape=lambda df: df["node"].map(org_segment_lookup))
            .assign(node_name=lambda df: df["node"])
            .assign(node_size=lambda df: df["node"].map(org_participations))
            # .dropna(axis=0, subset=['node_color'])
        )

        net_chart = plot_altair_network(
            node_df,
            net_conn,
            node_color="node_color",
            node_label="node_name",
            node_size="node_size",
            node_shape="node_shape",
            show_neighbours=True,
            edge_opacity=0.08,
            node_size_title="Number of participations",
            edge_weight_title="Number of collaborations",
            node_shape_title="Segment",
            title=f"{createch_name_lookup[category]} collaboration network",
            node_color_title="research community",
        ).properties(width=700, height=400)

        return net_chart
    else:
        return net


# -

data_net = make_category_network(
    org_proj_link_createch,
    projects_createch,
    org_segment_lookup,
    category="ai_data",
    thres=0,
).configure_view(strokeWidth=0)

save_altair(data_net, "data_network", driver=driv, path=FIG_PATH)

change_font_sizes(data_net)

content_net = make_category_network(
    org_proj_link_createch,
    projects_createch,
    org_segment_lookup,
    category="content_media",
    thres=0,
).configure_view(strokeWidth=0)

save_altair(
    change_font_sizes(content_net), "content_network", driver=driv, path=FIG_PATH
)

content_net

# ### Report statistics

len(projects)

max(projects.start)

# +
from createch.getters.gtr import get_organisations

orgs = get_organisations(creative=False)
# -

orgs["sim_mean"].describe()

len(get_gtr_topics().columns)

np.sum(orgs["creativecolumnsctor"] != "Other")

# +
sum(projects_createch["has_createch"] == True)

len(orgs_createch.query("has_createch==True").query("creative_sector=='Other'"))

# +
# Test network
# -

projects_createch["ci_benchmark"] = projects_createch["has_createch"] == False

# +
# org_projects_link_createch
# -

bench = make_network_from_coocc(
    make_cat_co_occ(org_proj_link_createch, projects_createch, "ci_benchmark")[0],
    spanning=False,
)

cat_nets = [
    make_network_from_coocc(
        make_cat_co_occ(org_proj_link_createch, projects_createch, seg)[0],
        spanning=False,
    )
    for seg in createch_labels
]


def get_network_stats(net):

    conn_comps = nx.connected_components(net)

    largest_cc = max(conn_comps, key=len)
    net_conn = net.subgraph(largest_cc)
    average_path_length = nx.average_shortest_path_length(net_conn)

    num_conn_comps = len(list(nx.connected_components(net)))


clust = [get_network_stats(net) for net in cat_nets]

d = [d[1] for d in cat_nets[3].degree()]
hh(d)

d = pd.DataFrame([d for d in cat_nets[2].degree()]).assign(
    segment=lambda df: df[0].map(org_segment_lookup)
)
d.groupby("segment")[1].mean()

eig = (
    pd.Series(nx.eigenvector_centrality(cat_nets[2]))
    .reset_index(name="score")
    .assign(segment=lambda df: df["index"].map(org_segment_lookup))
)
eig.groupby("segment")["score"].mean()


def hh(vector):

    hh = sum([x / sum(vector) ** 2 for x in vector])
    return hh


nx.density(cat_nets[4])

entropy([1, 1, 1, 1])

entropy([4, 1, 0, 0])

projects_w_text = projects.dropna(axis=0, subset=["abstractText"])

# +
projects_w_text["has_covid"] = [
    any(c in abstr.lower() for c in ["coronavirus", "covid", " sars-ncov", " sars "])
    for abstr in projects_w_text["abstractText"]
]

projects_w_text["has_createch"] = [
    _id in createch_project_ids for _id in projects_w_text["project_id"]
]

# +
projects_w_text_recent = projects_w_text.loc[
    [x.year == 2020 for x in projects_w_text["start"]]
]

pd.crosstab(
    projects_w_text_recent["has_covid"],
    projects_w_text_recent["has_createch"],
    normalize=1,
)
# -

projects_w_text_recent.query("has_covid==True").query("has_createch==True").sample(10)[
    "title"
]
