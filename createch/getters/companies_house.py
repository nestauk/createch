import logging
from functools import lru_cache

import pandas as pd
from metaflow import namespace, Run

import createch

logger = logging.getLogger(__name__)
namespace(None)


RUN_ID: int = createch.config["flows"]["companies_house"]["run_id"]


@lru_cache()
def _flow(run_id: int) -> Run:
    return Run(f"CompaniesHouseMergeDumpFlow/{run_id}")


def get_organisation() -> pd.DataFrame:
    return _flow(RUN_ID).data.organisation


def get_address() -> pd.DataFrame:
    return (
        _flow(RUN_ID)
        .data.organisationaddress.drop_duplicates(["company_number", "address_id"])
        .merge(_flow(RUN_ID).data.address, on="address_id")
    )


def get_sector() -> pd.DataFrame:
    """Returns most up-to-date sector rankings."""
    return (
        _flow(RUN_ID)
        .data.organisationsector.sort_values("date")
        .drop_duplicates(["company_number", "rank"], keep="last")
        .assign(SIC4_code=lambda x: x.sector_id.str.slice(0, 4))
        .rename(columns={"date": "data_dump_date", "sector_id": "SIC5_code"})
    )


def get_name() -> pd.DataFrame:
    """Returns companies house organisation name data."""
    return _flow(RUN_ID).data.organisationname
