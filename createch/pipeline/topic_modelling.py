import logging

import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
import tomotopy as tp
from statsmodels.regression.linear_model import OLS

from createch.hSBM_Topicmodel.sbmtm import sbmtm


def train_model(corpus, doc_ids):
    """Trains top sbm model on tokenised corpus"""
    model = sbmtm()
    model.make_graph(corpus, documents=doc_ids)
    model.fit()
    return model


def post_process_model(model, top_level, top_words=5):
    """Function to post-process the outputs of a hierarchical topic model
    _____
    Args:
      model: A hsbm topic model
      top_level: The level of resolution at which we want to extract topics
      top_words: top_words to include in the topic name
    _____
    Returns:
      A topic mix df with topics and weights by document
    """
    # Extract the word mix (word components of each topic)
    logging.info("Creating topic names")
    word_mix = model.topics(l=top_level)

    # Tidy names
    topic_name_lookup = {
        key: "_".join([x[0] for x in values[:top_words]])
        for key, values in word_mix.items()
    }
    topic_names = list(topic_name_lookup.values())

    # Extract the topic mix df
    logging.info("Extracting topics")
    topic_mix_ = pd.DataFrame(
        model.get_groups(l=top_level)["p_tw_d"].T,
        columns=topic_names,
        index=model.documents,
    )

    return topic_mix_


def filter_topics(topic_df, presence_thr, prevalence_thr):
    """Filter uninformative ("stop") topics
    Args:
        top_df (df): topics
        presence_thr (int): threshold to detect topic in article
        prevalence_thr (int): threshold to exclude topic from corpus
    Returns:
        Filtered df
    """
    # Remove highly uninformative / generic topics

    topic_prevalence = (
        topic_df.applymap(lambda x: x > presence_thr)
        .mean()
        .sort_values(ascending=False)
    )

    # Filter topics
    filter_topics = topic_prevalence.index[topic_prevalence > prevalence_thr].tolist()

    # We also remove short topics (with less than two ngrams)
    filter_topics = filter_topics + [
        x for x in topic_prevalence.index if len(x.split("_")) <= 2
    ]

    topic_df_filt = topic_df.drop(filter_topics, axis=1)

    return topic_df_filt, filter_topics


def post_process_model_clusters(model, top_level, cl_level, top_thres=1, top_words=5):
    """Function to post-process the outputs of a hierarchical topic model
    _____
    Args:
        model: A hsbm topic model
        top_level: The level of resolution at which we want to extract topics
        cl_level:The level of resolution at which we want to extract clusters
        top_thres: The maximum share of documents where a topic appears.
        1 means that all topics are included
        top_words: number of words to use when naming topics

    _____
    Returns:
      A topic mix df with topics and weights by document
      A lookup between ids and clusters
    """
    # Extract the word mix (word components of each topic)
    topic_mix_ = post_process_model(model, top_level, top_words)

    # word_mix = model.topics(l=top_level)

    # # Create tidier names
    # topic_name_lookup = {
    #     key: "_".join([x[0] for x in values[:5]]) for key, values in word_mix.items()
    # }
    # topic_names = list(topic_name_lookup.values())

    # # Extract the topic mix df
    # topic_mix_ = pd.DataFrame(
    #     model.get_groups(l=top_level)["p_tw_d"].T,
    #     columns=topic_names,
    #     index=model.documents,
    # )

    # Remove highly uninformative / generic topics
    topic_prevalence = (
        topic_mix_.applymap(lambda x: x > 0).mean().sort_values(ascending=False)
    )
    filter_topics = topic_prevalence.index[topic_prevalence < top_thres]
    topic_mix = topic_mix_[filter_topics]

    # Extract the clusters to which different documents belong (we force all documents
    # to belong to a cluster)
    cluster_assignment = model.clusters(l=cl_level, n=len(model.documents))
    # cluster_sets = {
    #     c: set([x[0] for x in papers]) for c, papers in cluster_assigment.items()
    # }

    # # Assign topics to their clusters
    # topic_mix["cluster"] = [
    #     [f"cluster_{n}" for n, v in cluster_sets.items() if x in v][0]
    #     for x in topic_mix.index
    # ]

    return topic_mix, cluster_assignment


def get_topic_words(topic, top_words=5):
    """Extracts main words for a topic"""

    return "_".join([x[0] for x in topic[:top_words]])


def make_topic_mix(mdl, num_topics, doc_indices):
    """Takes a tomotopy model and products a topic mix"""
    topic_mix = pd.DataFrame(
        np.array([mdl.docs[n].get_topic_dist() for n in range(len(doc_indices))])
    )

    topic_mix.columns = [
        get_topic_words(mdl.get_topic_words(n, top_n=5)) for n in range(num_topics)
    ]

    topic_mix.index = doc_indices
    return topic_mix


def parse_var_name(var):
    """Parses regression outputs when using a reference class"""

    return var.split(".")[1][:-1].strip()


def topic_regression(topic_mix, cat_vector, ref_class, log=True):
    """Carries out a topic regression using a vector of categorical variables as
    predictors. Equivalent to a comparison of means between a category and the
    reference class
    """

    results = []

    for t in topic_mix.columns:

        # We log the results to account for skewedness
        top = topic_mix[t].reset_index(drop=True)

        min_val = top.loc[top > 0].min()

        # We create a floor above zero so we can log all topic values
        if log is True:
            zero_rep = (0 + min_val) / 2

            y = [np.log(x) if x > 0 else np.log(zero_rep) for x in top]
        else:
            y = top.copy()

        reg_df = pd.DataFrame({"y": y, "x": cat_vector})

        if ref_class != None:
            m = smf.ols(
                formula=f"y ~ C(x, Treatment(reference='{ref_class}'))", data=reg_df
            ).fit()
            confint = m.conf_int().assign(label=t).iloc[1:, :].reset_index(drop=False)
            confint["index"] = [parse_var_name(v) for v in confint["index"]]
            results.append(confint)

        else:

            confint = []
            exog = pd.get_dummies(reg_df["x"])
            for e in exog.columns:
                one_var = sm.add_constant(exog[e])
                m = OLS(endog=reg_df["y"], exog=one_var).fit()
                confint_one = (
                    m.conf_int().assign(label=t).iloc[1:, :].reset_index(drop=False)
                )
                confint.append(confint_one)
            results.append(pd.concat(confint))

    return results


def train_topic_model(k, texts, ids, iters=100):
    """Train topic model while grid searching"""
    logging.info(f"training model with {k} topics")

    mdl = tp.LDAModel(k=k)

    for t in texts:

        mdl.add_doc(t)

    for _ in range(0, iters, 10):
        mdl.train(10)

    return mdl, ids
