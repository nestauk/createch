"""Prepare organisation name data for `JacchammerFlow`.

Converts Companies House and Crunchbase name data to JSON files required
by `JacchammerFlow`.
"""
import json
from pathlib import Path

from createch import PROJECT_DIR
from createch.getters.companies_house import get_name as get_ch
from createch.getters.crunchbase import get_name as get_cb


if __name__ == "__main__":
    cache_dir = Path(f"{PROJECT_DIR}/outputs/.cache")
    cache_dir.mkdir(exist_ok=True, parents=True)

    with open(cache_dir / "company_names.json", "w") as f:
        json.dump(
            get_ch()[["company_number", "name"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .set_index("company_number")
            .name.to_dict(),
            f,
        )

    with open(cache_dir / "cb_names.json", "w") as f:
        json.dump(get_cb(), f)
