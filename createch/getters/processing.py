# Getters for use while processing data

import json
import pickle

from createch import PROJECT_DIR


def save_tokenised(lookup: dict, name: str):
    """Saves lookup betweeen ids and tokenised descriptions"""

    if "crunchbase" in name:
        with open(f"{PROJECT_DIR}/outputs/data/crunchbase/{name}.json", "w") as outfile:
            json.dump(lookup, outfile)
    elif "gtr" in name:
        with open(f"{PROJECT_DIR}/outputs/data/gtr/{name}.json", "w") as outfile:
            json.dump(lookup, outfile)


def get_tokenised(name: dict):
    """Reads lookup betweeen ids and tokenised descriptions"""

    if "crunchbase" in name:
        with open(f"{PROJECT_DIR}/outputs/data/crunchbase/{name}.json", "r") as infile:
            tok = json.load(infile)
    elif "gtr" in name:
        with open(f"{PROJECT_DIR}/outputs/data/gtr/{name}.json", "r") as infile:
            tok = json.load(infile)
    return tok


def save_model(model, path):
    """Serialises model"""
    with open(f"{PROJECT_DIR}/{path}.p", "wb") as outfile:
        pickle.dump(model, outfile)


def get_model(path):
    """Serialises model"""
    with open(f"{PROJECT_DIR}/{path}.p", "rb") as infile:
        return pickle.load(infile)
