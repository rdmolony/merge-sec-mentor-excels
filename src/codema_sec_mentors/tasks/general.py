from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from time import strptime
from typing import List, Union, Dict
from copy import copy

from icontract import require, ensure
import numpy as np
from openpyxl import load_workbook, Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.cell.cell import Cell
import pandas as pd
from prefect import task


@task
def _get_excel_filepaths(directory: Path, mentor_las: List[Path]) -> List[Path]:

    _all_mentor_dir_excel_files = directory.rglob("*.xlsx")
    return list(
        set(
            [
                filepath
                for filepath in _all_mentor_dir_excel_files
                for la in mentor_las
                if la in filepath.stem
            ]
        )
    )  # Using list(set()) as a hacky way of removing duplicate filepaths...


@task
def _get_excel_filepath(directory: Path, file_number) -> List[Path]:

    _all_mentor_dir_excel_files = directory.rglob("*.xlsx")
    return list(_all_mentor_dir_excel_files)[file_number]


@task
def _replace_question_marks_with_nan(df: pd.DataFrame) -> pd.DataFrame:

    return df.replace({"?": np.nan})


@task
def _drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:

    return df.dropna(how="all")


@task
def _concatenate_data_from_multiple_sheets(
    excel_dfs: List[pd.DataFrame],
) -> pd.DataFrame:

    return pd.concat(excel_dfs, ignore_index=True)
