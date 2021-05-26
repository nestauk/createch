import json
import logging
from functools import lru_cache
from typing import Dict

import pandas as pd
from metaflow import namespace, Run

import createch
from createch import PROJECT_DIR
from createch.pipeline.fetch_daps1_data.cb_utils import CB_PATH

logger = logging.getLogger(__name__)
namespace(None)

RUN_ID: int = createch.config["flows"]["nesta"]["run_id"]


def get_crunchbase_orgs():
    return pd.read_csv(f"{CB_PATH}/crunchbase_organisations.csv")


def get_crunchbase_tokenised():
    with open(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_tokenised.json", "r"
    ) as infile:
        return json.load(infile)


def get_crunchbase_tagged():
    with open(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_area_tagged.json", "r"
    ) as infile:
        return json.load(infile)


def get_crunchbase_vocabulary():
    with open(
        f"{PROJECT_DIR}/outputs/data/crunchbase/crunchbase_vocabulary.json", "r"
    ) as infile:
        return json.load(infile)


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CreatechNestaGetter/{run_id}")


def get_name() -> Dict[str, str]:
    """Lookup between Crunchbase organisation ID and name."""
    return _flow(RUN_ID).data.crunchbase_names
