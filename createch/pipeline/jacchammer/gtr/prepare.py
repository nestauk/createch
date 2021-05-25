"""Prepare organisation name data for `JacchammerFlow`.

Converts Companies House and GtR name data to JSON files required
by `JacchammerFlow`.
"""
import json

from createch import PROJECT_DIR
from createch.getters.companies_house import get_name as get_ch
from createch.getters.gtr import get_name as get_gtr


if __name__ == "__main__":
    with open(f"{PROJECT_DIR}/outputs/.cache/company_names.json", "w") as f:
        json.dump(
            get_ch()[["company_number", "name"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .set_index("company_number")
            .name.to_dict(),
            f,
        )

    with open(f"{PROJECT_DIR}/outputs/.cache/gtr_names.json", "w") as f:
        json.dump(get_gtr(), f)
