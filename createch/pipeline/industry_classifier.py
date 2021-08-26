# Build discipline labelled dataset and classify projects into disciplines

import logging
import warnings

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, make_scorer


from createch import config, PROJECT_DIR
from createch.getters.crunchbase import (
    get_cb_ch_organisations,
    get_crunchbase_orgs,
    get_crunchbase_topics,
)
from createch.pipeline.prediction_utils import (
    grid_search,
    make_doc_term_matrix,
    make_predicted_label_df,
    parse_parametres,
)
from createch.pipeline.utils import make_org_description_lu

warnings.filterwarnings("ignore", category=DeprecationWarning)


def make_labelled_dataset(orgs_ch, cb_orgs):
    orgs_ch = get_cb_ch_organisations(creative=True)

    orgs_ch["combined_description"] = orgs_ch["cb_id"].map(
        make_org_description_lu(cb_orgs)
    )

    orgs_ch = orgs_ch.dropna(axis=0, subset=["combined_description"])
    orgs_ch_long = orgs_ch.loc[[len(x) > 75 for x in orgs_ch["combined_description"]]]

    orgs_ch_long = orgs_ch_long.loc[
        ~orgs_ch_long["creative_sector"].isin(
            ["Crafts", "Museums galleries and libraries"]
        )
    ]
    return orgs_ch_long


def label_all_creative(orgs_ch, cb_orgs, vect_fit, best_estimator, y_cols):
    """Label crunchbase predicted labels (only if we are outside the labelled dataset)"""

    all_creative = cb_orgs.loc[
        cb_orgs["id"].isin(set(get_crunchbase_topics().index))
    ].drop_duplicates("id")

    all_creative["combined_description"] = all_creative["id"].map(
        make_org_description_lu(cb_orgs)
    )

    pred_creative = make_predicted_label_df(
        all_creative,
        vect_fit,
        best_estimator,
        y_cols,
        text_var="combined_description",
        id_var="id",
        min_length=75,
    )

    org_creative_sector_lookup = orgs_ch.set_index("cb_id")["creative_sector"].to_dict()

    ch_sector = pred_creative.index.map(org_creative_sector_lookup)

    pred_creative["sector_label"] = [
        ch_sector
        if (pd.isnull(ch_sector) == False) & (ch_sector != "other")
        else r[1].idxmax()
        for r, ch_sector in zip(pred_creative.iterrows(), ch_sector)
    ]

    return pred_creative


def predict_industries():
    logging.info("Making labelled dataset")
    orgs_ch = get_cb_ch_organisations(creative=True)
    cb_orgs = get_crunchbase_orgs()

    labelled_dataset = make_labelled_dataset(orgs_ch, cb_orgs)

    model_parametres = parse_parametres(
        config["discipline_classification"]["model_parametres"]
    )

    Y = np.array(pd.get_dummies(labelled_dataset["creative_sector"]))
    y_cols = sorted(set(labelled_dataset["creative_sector"]))

    vect_fit, X_proc = make_doc_term_matrix(
        labelled_dataset["combined_description"], CountVectorizer, max_features=20000
    )

    f1_multi = make_scorer(f1_score, average="weighted")

    results = []
    models = [
        LogisticRegression(solver="liblinear"),
        RandomForestClassifier(),
        #    GradientBoostingClassifier(),
    ]
    names = ["logistic", "random_forest"]

    logging.info("Training models")

    for mod, pars, name in zip(models, model_parametres, names):
        logging.info(f"grid searching {name}")
        clf = grid_search(X_proc, Y, mod, pars, f1_multi)
        results.append(clf)

    scores = [r.best_score_ for r in results]
    index_best = scores.index(max(scores))

    logging.info(f"Best classifier is {names[index_best]}")

    best_estimator = results[index_best].best_estimator_
    logging.info(f"{best_estimator}")

    logging.info("Predicting labels")
    labelled_all = label_all_creative(
        orgs_ch, cb_orgs, vect_fit, best_estimator, y_cols
    )

    labelled_all.to_csv(
        f"{PROJECT_DIR}/outputs/data/crunchbase/predicted_industries.csv"
    )


if __name__ == "__main__":
    predict_industries()
