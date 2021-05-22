# Getters for use while processing data

import json

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
            return json.load(infile)
    elif "gtr" in name:
        with open(f"{PROJECT_DIR}/outputs/data/gtr/{name}.json", "w") as infile:
            return json.load(infile)
