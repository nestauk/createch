import json
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from metaflow import (
    conda_base,
    current,
    FlowSpec,
    IncludeFile,
    Parameter,
    step,
)


@conda_base(libraries={"cytoolz": "0.11", "pandas": ">=1", "pytables": ">=3.6.1"})
class JacchammerFlow(FlowSpec):

    input_x = IncludeFile("names-x", help="names x")
    input_y = IncludeFile("names-y", help="names y")

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )
    clean_names = Parameter(
        "clean_names",
        help="Whether to clean names using Jacchammer default `preproc_names`",
        type=bool,
        default=False,
    )

    @step
    def start(self):
        """Load raw data"""
        import pandas as pd

        self.run_id = current.origin_run_id or current.run_id

        nrows = 10_000 if self.test_mode else None

        self.names_x = pd.Series(json.loads(self.input_x), name="names_x").head(nrows)
        self.names_y = pd.Series(json.loads(self.input_y), name="names_y").head(nrows)

        self.next(self.match)

    @step
    def match(self):
        """The core fuzzy matching algorithm"""
        from cytoolz.curried import curry, pipe

        os.system(
            f"{sys.executable} -m pip install --quiet "
            "git+https://github.com/nestauk/jacc-hammer.git@legacy"
        )
        from jacc_hammer.fuzzy_hash import (
            Cos_config,
            Fuzzy_config,
            match_names_stream,
            stream_sim_chunks_to_hdf,
        )
        from jacc_hammer.top_matches import get_top_matches_chunked
        from jacc_hammer.name_clean import preproc_names

        self.names_x = self.names_x.pipe(
            lambda x: preproc_names(x) if self.clean_names else x
        )
        self.names_y = self.names_y.pipe(
            lambda x: preproc_names(x) if self.clean_names else x
        )

        self.tmp_dir = Path(TemporaryDirectory(dir=".").name)
        self.tmp_dir.mkdir()

        cos_config = Cos_config()
        fuzzy_config = Fuzzy_config(num_perm=128)
        match_config = dict(
            threshold=33,
            chunksize=100,
            cos_config=cos_config,
            fuzzy_config=fuzzy_config,
            tmp_dir=self.tmp_dir,
        )
        chunksize = 1e7

        f_fuzzy_similarities = f"{self.tmp_dir}/fuzzy_similarities"
        _ = pipe(
            [self.names_x.values, self.names_y.values],
            curry(match_names_stream, **match_config),
            curry(stream_sim_chunks_to_hdf, fout=f_fuzzy_similarities),
        )

        self.top_matches = get_top_matches_chunked(
            f_fuzzy_similarities, chunksize=chunksize, tmp_dir=self.tmp_dir
        )

        self.next(self.end)

    @step
    def end(self):
        """Merge names and id's back in and convert to dict."""
        self.full_top_matches = (
            self.top_matches.merge(
                self.names_y.reset_index().rename(columns={"index": "index_y"}),
                left_on="y",
                right_index=True,
                validate="1:1",
            )
            .merge(
                self.names_x.reset_index().rename(columns={"index": "index_x"}),
                left_on="x",
                right_index=True,
                validate="m:1",
            )
            .drop(["x", "y"], axis=1)
            .to_dict()
        )


if __name__ == "__main__":
    JacchammerFlow()
