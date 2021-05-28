# Fetch GtR tables
# TODO: Add organisations and funding tables
import logging
from functools import lru_cache
from typing import Dict

from metaflow import namespace, Run

import createch

logger = logging.getLogger(__name__)

namespace(None)

RUN_ID: int = createch.config["flows"]["nesta"]["run_id"]


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CreatechNestaGetter/{run_id}")


def get_name() -> Dict[str, str]:
    """Lookup between GtR organisation ID and name."""
    return _flow(RUN_ID).data.gtr_names
