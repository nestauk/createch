# Scripts to identify relevant projects in descriptions

import json
import logging

import gensim
import numpy as np

from createch import PROJECT_DIR
from createch.getters.processing import get_model, get_tokenised


with open(f"{PROJECT_DIR}/inputs/data/stop_terms.txt", "r") as infile:
    STOP_TERMS = infile.read().split(", ")


def get_vocabulary():
    with open(f"{PROJECT_DIR}/inputs/data/createch_vocabulary.json", "r") as infile:
        return json.load(infile)


def save_vocabulary(voc: dict, source: str):
    """saves an expanded vocabulary
    Args:
        voc: the vocabulary
        source: gtr or crunchbase
    """
    with open(
        f"{PROJECT_DIR}/outputs/data/{source}/{source}_vocabulary.json", "w"
    ) as outfile:
        json.dump(voc, outfile)


def save_matches(matched_dict: dict, source: str):
    """Saves a lookup between companies and expanded matches
    Args:
        matched_dict: the lookup
        source: the data source
    """
    with open(
        f"{PROJECT_DIR}/outputs/data/{source}/{source}_area_tagged.json", "w"
    ) as outfile:
        return json.dump(matched_dict, outfile)


def make_expansions(
    word: str,
    w2v,
    stop_terms: list = STOP_TERMS,
    thres: float = 0.75,
) -> list:
    """Expand terms from an initial set.
    Args:
        word: seed terms
        w2v: word2vec model
        stop_terms: list of terms to remove
        thres: similarity threshold
    Returns:
        A list of terms that are semantically similar to the seed
    """
    try:
        most_similar = w2v.wv.most_similar(word, topn=20)
        most_similar_high = [
            w[0] for w in most_similar if (w[1] > thres) & (w[0] not in stop_terms)
        ]
        return most_similar_high
    except KeyError:
        print("word not in vocabulary")
        return [np.nan]


def flatten(_list: list, missing: float = np.nan):
    """Flattens a list ignoringg missing values (terms not in vocabulary)"""
    return [w for el in _list for w in el if w is not missing]


def count_vocab_matches(token_descr: list, expanded_voc: dict) -> dict:
    """Count number of matches between the expanded vocabulary and a project /
     company description
    Args:
        token_descr: list of tokens about what the organisation does
        expanded_dict: expanded dictionary of terms
    Returns:
        Dict with number of occurrences from different categories in our list
    """
    matches = {}

    for k, v in expanded_voc.items():

        if len(set(v) & set(token_descr)) > 0:
            matches[k] = [
                (w, token_descr.count(w)) for w in v if token_descr.count(w) > 0
            ]

    return matches


def make_semantic_labels(source):
    """ """
    logging.info("Getting data")
    tokenised = get_tokenised(f"{source}_tokenised")
    w2v = get_model(f"outputs/models/{source}/{source}_w2v")
    vocab = get_vocabulary()

    logging.info("Finding matches")
    expanded_voc = {
        k: list(
            set(v).union(
                set(
                    flatten([make_expansions(w, w2v, STOP_TERMS, thres=0.8) for w in v])
                )
            )
        )
        for k, v in vocab.items()
    }
    all_vocab_matches = {
        _id: count_vocab_matches(descr, expanded_voc)
        for _id, descr in tokenised.items()
    }

    logging.info("Saving data")
    save_vocabulary(expanded_voc, source)
    save_matches(all_vocab_matches, source)


if __name__ == "__main__":
    for source in ["gtr", "crunchbase"]:
        logging.info(source)
        make_semantic_labels(source)
