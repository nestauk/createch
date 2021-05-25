import logging
from functools import lru_cache

import pandas as pd
from metaflow import namespace, Run

import createch

logger = logging.getLogger(__name__)
namespace(None)


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"JacchammerFlow/{run_id}")


def get_gtr(run_id=None) -> pd.DataFrame:
    """Return GtR-Companies House matches."""
    _run_id: int = run_id or createch.config["flows"]["jacchammer"]["gtr"]["run_id"]
    return pd.DataFrame(_flow(_run_id).data.full_top_matches).rename(
        columns={
            "index_y": "gtr_id",
            "names_y": "gtr_name",
            "index_x": "company_number",
            "names_x": "ch_name",
        }
    )


def get_crunchbase(run_id=None) -> pd.DataFrame:
    """Return Crunchbase-Companies House matches."""
    _run_id: int = (
        run_id or createch.config["flows"]["jacchammer"]["crunchbase"]["run_id"]
    )
    return pd.DataFrame(_flow(_run_id).data.full_top_matches).rename(
        columns={
            "index_y": "cb_id",
            "names_y": "cb_name",
            "index_x": "company_number",
            "names_x": "ch_name",
        }
    )
