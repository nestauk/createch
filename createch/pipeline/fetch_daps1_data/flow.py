"""Metaflow to fetch data from DAPS1 (`nestauk/nesta`).

GtR and Crunchbase data pipeline currently reside `nestauk/nesta`, and are
fetched using the `nestauk/data_getters` internal library.

`nestauk/data_getters` is a private library which will prompt for
authentication to store unless your Github 2FA password is cached.
It pulls in all the dependencies of `nestauk/nesta` which are heavy
and may conflict with newer packages hence, this fetching is encapsulated
within a metaflow.
"""
import os
import sys
from typing import Dict

from metaflow import conda, FlowSpec, Parameter, step


ENV_VAR = "MYSQL_CONFIG"


# TODO: refactor into `utils.daps1.py` when `getters.daps.py` is refactored
def get_names(con, table_name) -> Dict[str, str]:
    """Fetch non-null `{id: name}` pairs from `table_name`."""
    from pandas import read_sql_table

    return (
        read_sql_table(table_name, con, columns=["id", "name"])
        .set_index("id")
        .name.dropna()
        .to_dict()
    )


class CreatechNestaGetter(FlowSpec):

    db_config_path = Parameter("db-config-path", type=str, default=os.getenv(ENV_VAR))

    @step
    def start(self):
        self.next(self.fetch_names)

    @conda(python="3.7")
    @step
    def fetch_names(self):
        """Fetch Organisation (GtR & crunchbase) names."""
        os.system(
            f"{sys.executable} -m pip install --quiet "
            "git+ssh://git@github.com/nestauk/data_getters.git"
        )
        from data_getters.core import get_engine

        if self.db_config_path is None:
            raise ValueError(
                f"`db_config_path` was not set. Pass in a config path as a "
                f"flow argument or set {ENV_VAR} env variable."
            )

        engine = get_engine(self.db_config_path)
        con = engine.connect()

        self.gtr_names = get_names(con, "gtr_organisations")
        self.crunchbase_names = get_names(con, "crunchbase_organizations")

        self.next(self.fetch_cb)

    @conda(python="3.7")
    @step
    def fetch_cb(self):
        """Fetch Crunchbase tables."""
        os.system(
            f"{sys.executable} -m pip install --quiet "
            "git+ssh://git@github.com/nestauk/data_getters.git"
        )
        from cb_utils import CB_PATH, fetch_save_crunchbase

        if os.path.exists(CB_PATH) is False:
            os.makedirs(CB_PATH)

        fetch_save_crunchbase()

        self.next(self.fetch_gtr)

    @conda(python="3.7")
    @step
    def fetch_gtr(self):
        """Fetch GtR tables."""
        os.system(
            f"{sys.executable} -m pip install --quiet "
            "git+ssh://git@github.com/nestauk/data_getters.git"
        )
        from gtr_utils import GTR_PATH, fetch_save_gtr_tables

        if os.path.exists(GTR_PATH) is False:
            os.mkdir(GTR_PATH)

        fetch_save_gtr_tables()

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CreatechNestaGetter()
