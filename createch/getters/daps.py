import pandas as pd


def get_daps_table(name: str, path: str) -> pd.DataFrame:
    """Get DAPS table
    Args:
        name: table name
        path: storage path
    """
    table = pd.read_csv(f"{path}/{name}.csv")
    return table
