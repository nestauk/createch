# Tokenise and model project / company descriptions
import logging

import pandas as pd
from gensim.models import Word2Vec

from createch import config, PROJECT_DIR
from createch.getters.daps import get_daps_table
from createch.getters.processing import save_model, save_tokenised
from createch.pipeline.text_pre_processing import pre_process_corpus
from createch.utils.path_utils import make_dir

# Pars
files = ["gtr", "crunchbase"]
TOK_PARS = config["tokenise"]

# TODO: REMOVE THE TEST


def read_process_text(source: str) -> dict:
    """Reads and processes text description
    Args:
        source: source we are processing
    Returns:
        A dict where keys are ids and elements are tokenised descriptions
    """

    logging.info(f"Reading {source}")
    if source == "crunchbase":

        table = get_daps_table(
            "crunchbase_organisations", f"{PROJECT_DIR}/inputs/data/crunchbase"
        ).drop_duplicates(subset=["id"])
        logging.info(len(table))

        table["description"] = [
            row["long_description"]
            if pd.isnull(row["long_description"]) is False
            else row["short_description"]
            for _id, row in table.iterrows()
        ]

        id_text_lookup = (
            table.dropna(axis=0, subset=["description"])
            .set_index("id")["description"]
            .to_dict()
        )

    elif source == "gtr":

        table = get_daps_table("gtr_projects", f"{PROJECT_DIR}/inputs/data/gtr")
        id_text_lookup = (
            table.dropna(axis=0, subset=["abstractText"])
            .set_index("project_id")["abstractText"]
            .to_dict()
        )

    tokenised = pre_process_corpus(id_text_lookup.values(), **TOK_PARS)
    id_tokenised_lookup = {k: t for k, t in zip(id_text_lookup.keys(), tokenised)}

    logging.info(f"Saving {source}")
    save_tokenised(id_tokenised_lookup, f"{source}_tokenised")

    return id_tokenised_lookup


if __name__ == "__main__":

    for source in ["gtr", "crunchbase"]:
        make_dir(f"outputs/data/{source}")
        make_dir(f"outputs/models/{source}")

        tokenised_lookup = read_process_text(source)

        logging.info("Training w2vec model")
        w2v = Word2Vec(list(tokenised_lookup.values()))

        logging.info("Saving w2vec model")
        save_model(w2v, f"outputs/models/{source}/{source}_w2v")
