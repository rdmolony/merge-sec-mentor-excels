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
        if ("SEC" in file.stem)
        and ("~$" not in file.stem)
        and ("(1)" not in file.stem)
        and ("(2)" not in file.stem)
        and ("(3)" not in file.stem)
        and ("html" not in file.stem)
    ]


@task
def read_excel_to_dict(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_excel(filepath, sheet_name=None)


@task
def regroup_excels_by_sheet(
    local_authority_excels: List[Dict[str, pd.DataFrame]]
) -> Dict[str, pd.DataFrame]:

    return merge_with(list, *local_authority_excels)
