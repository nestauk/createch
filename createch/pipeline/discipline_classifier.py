# Build discipline labelled dataset and classify projects into disciplines

import logging

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, make_scorer, precision_score
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import MultiLabelBinarizer


from createch import config, PROJECT_DIR
from createch.pipeline.make_research_topic_partition import make_network_analysis_inputs
from createch.utils.io import get_lookup


def parse_parametres(parametre_list: list) -> list:
    """Parse Nones in the parameter dict"""
    parametre_copy = []

    for el in parametre_list:
        new_dict = {}
        for k, v in el.items():
            new_dict[k] = [par if par != "None" else None for par in v]
        parametre_copy.append(new_dict)

    return parametre_copy


def make_labelled_dataset(
    categories_projects: pd.DataFrame, projects: pd.DataFrame, discipline_names: dict
) -> pd.DataFrame:
    """Create labelled dataset"""
    categories_projects = categories_projects.reset_index(name="categories")
    categories_projects["discipline_list"] = [
        [discipline_names[c] for c in cat] for cat in categories_projects["categories"]
    ]

    categories_projects_pure = categories_projects.loc[
        [len(d) == 1 for d in categories_projects["discipline_list"]]
    ].assign(single_discipline=lambda df: df["discipline_list"].apply(lambda x: x[0]))

    project_discipline_lookup = categories_projects_pure.set_index("project_id")[
        "single_discipline"
    ].to_dict()

    # Label medical projects
    med_lookup = {
        row["project_id"]: "medical"
        for _, row in projects.iterrows()
        if row["leadFunder"] == "MRC"
    }

    project_discipline_lookup = dict(**project_discipline_lookup, **med_lookup)

    project_labelled = (
        projects["project_id"]
        .loc[[len(x) > 300 for x in projects["abstractText"]]]
        .map(project_discipline_lookup)
        .dropna(axis=0, subset=["single_discipline"])
        .reset_index(drop=True)
    )

    return project_labelled


def make_tfidf(training_features):

    # Create and apply tfidf transformer
    tfidf_vect = TfidfVectorizer(
        ngram_range=[1, 2], stop_words="english", max_features=30000
    )
    tfids_fit = tfidf_vect.fit(training_features)

    # Create processed text

    X_train_proc = tfids_fit.fit_transform(X_train)

    return tfids_fit, X_train_proc


def grid_search(X, y, model, parametres, metric):

    estimator = OneVsRestClassifier(model)
    clf = GridSearchCV(estimator, parametres, scoring=metric, cv=3)
    clf.fit(X, y)
    return clf


if __name__ == "__main__":

    logging.info("Reading data")
    projects, categories_projects = make_network_analysis_inputs()

    comm_project_lookup = get_lookup("outputs/data/gtr/topic_community_lookup")
    discipline_names = config["discipline_classification"]["discipline_names"]
    comm_discipline_names = {
        k: discipline_names[v] for k, v in comm_project_lookup.items()
    }

    logging.info("Creating labelled dataset")
    project_labelled = make_labelled_dataset(
        categories_projects, projects, comm_discipline_names
    )

    # Classification and validation
    logging.info("Starting modelling")
    model_parametres = parse_parametres(
        config["discipline_classification"]["model_parametres"]
    )

    Y = MultiLabelBinarizer().fit_transform(project_labelled["single_discipline"])
    X_train, X_test, y_train, y_test = train_test_split(
        project_labelled["abstractText"], Y
    )

    tfids_fit, X_train_proc = make_tfidf(X_train)

    f1_multi = make_scorer(f1_score, average="weighted")
    precision_multi = make_scorer(precision_score, average="micro")

    results = []
    models = [
        LogisticRegression(solver="liblinear"),
        RandomForestClassifier(),
        GradientBoostingClassifier(),
    ]
    names = ["logistic", "random_forest", "gradient_boost"]

    for mod, pars, name in zip(models, model_parametres, names):
        logging.info(f"grid searching {name}")
        clf = grid_search(X_train_proc, y_train, mod, pars, precision_multi)
        results.append(clf)
