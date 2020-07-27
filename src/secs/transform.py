from typing import Dict

import pandas as pd
import prefect
from prefect import task


def _clean_data(df: pd.DataFrame, ref_column: str) -> pd.DataFrame:

    return df.replace({"?": np.nan}).dropna(subset=[ref_column])


@task
def transform_mentor_excels(
    mentor_excels_mapped: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:

    import ipdb

    ipdb.set_trace()
