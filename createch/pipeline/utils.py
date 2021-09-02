# Various utilities that we use across the pipeline
import pandas as pd
from IPython.display import display


def make_org_description_lu(orgs):

    descr = {
        r["id"]: r["long_description"]
        if pd.isnull(r["long_description"]) is False
        else r["short_description"]
        for _, r in orgs.iterrows()
    }
    return descr


def has_createch_sector(comm_number, vector, comm_container, thres=0.04):
    """Checks if a project has topics from a createch sector"""

    has_comm_number = any(vector[top] > thres for top in comm_container[comm_number])
    if has_comm_number is True:
        return True
    else:
        return False


def preview(x, nrows=5, T=False, out=display):
    """Print a preview of DataFrame and return input

    Args:
        x (pandas.DataFrame): DataFrame to preview
        nrows (int, optional): Number of rows of head and tail to show
        T (bool, optional): Whether to print transposed
        out (function, optional): Method to display preview with.
            Defaults to ipython's display.

    Returns:
        pandas.DataFrame
    """

    if nrows >= x.shape[0]:
        x_prev = x
    else:
        x_prev = pd.concat([x.head(nrows), x.tail(nrows)])

    if T:
        x_prev = x_prev.T

    out(x_prev)
    out(f"Shape: {x.shape}")

    return x
