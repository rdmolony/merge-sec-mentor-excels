import re
from pathlib import Path
from typing import Dict, List

import icontract
import numpy as np
import pandas as pd
from cytoolz.dicttoolz import merge_with
from prefect import task
import prefect


@task
def get_mentor_excel_filepaths(dirpath: Path) -> List[Path]:

    logger = prefect.context.get("logger")

    # get all excel spreadsheets in mentor dir but the test example ...
    # return [file for file in dirpath.rglob("*.xlsx") if file.stem not in ["test", "$~"]]

    import ipdb

    ipdb.set_trace()

    filepaths = [
        filepath
        for filepath in dirpath.rglob("*.xlsx")
        if ("SEC" in filepath.stem)
        and ("~$" not in filepath.stem)
        and not bool(
            re.search(r"\(\d\)", filepath.stem)
        )  # ignore all SEC(1).xlsx etc. as duplicates...
        and ("html" not in filepath.stem)
        and ("invalid" not in filepath.stem)
    ]

    if logger:
        mentor_excels = [filepath.stem for filepath in filepaths]
        logger.info(f"\n\nMentor Excels: {mentor_excels}\n\n")

    return filepaths


@task
def read_excel_to_dict(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_excel(filepath, sheet_name=None)


@task
def regroup_excels_by_sheet(
    local_authority_excels: List[Dict[str, pd.DataFrame]]
) -> Dict[str, pd.DataFrame]:

    return merge_with(list, *local_authority_excels)
