from pathlib import Path
from typing import Dict, List

from cytoolz.dicttoolz import merge_with

# from toolz.dicttoolz import merge_with
import icontract
import numpy as np
import pandas as pd
from prefect import task


@task
def get_mentor_excel_filepaths(dirpath: Path) -> List[Path]:

    # get all excel spreadsheets in mentor dir but the test example ...
    # return [file for file in dirpath.rglob("*.xlsx") if file.stem not in ["test", "$~"]]

    return [
        file
        for file in dirpath.rglob("*.xlsx")
        if not any(to_ignore in file.stem for to_ignore in ["test", "~$"])
    ]


@task
def read_excel_to_dict(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_excel(filepath, sheet_name=None)


@task
def regroup_excels_by_sheet(
    local_authority_excels: List[Dict[str, pd.DataFrame]]
) -> Dict[str, pd.DataFrame]:

    return merge_with(list, *local_authority_excels)
