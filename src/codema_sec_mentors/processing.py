from collections import defaultdict
from pathlib import Path
from typing import List

from icontract import require, ensure
from openpyxl import load_workbook
import pandas as pd
from prefect import Flow, task, unmapped, Parameter
from pipeop import pipes

from _filepaths import DATA_DIR, MENTOR_DIR
from _globals import MENTOR_LAS
from utilities.flow import run_flow


RESULTS_DIR = DATA_DIR / "results"

# ETL Flow
# ********


@pipes
def process_mentor_excels() -> Flow:

    with Flow("Process SEC mentor excels") as flow:

        filepaths = _get_excel_filepaths(MENTOR_DIR)
        hourly_overview_raw = _load_sec_activity_by_month_sheet_inferring_headers.map(
            filepaths
        )

        hourly_overview_clean = (
            _drop_empty_rows.map(hourly_overview_raw)
            >> _get_rlpdt_totals.map()
            >> _merge_mentor_dataframes
        )

        _save_merged_dataframe_to_excel(
            hourly_overview_clean, RESULTS_DIR, "mentor_hours.xlsx"
        )

    return flow


# Tasks
# *****


@task
def _get_excel_filepaths(directory) -> List[Path]:

    _all_mentor_dir_excel_files = directory.rglob("*.xlsx")
    return list(
        set(
            [
                filepath
                for filepath in _all_mentor_dir_excel_files
                for la in MENTOR_LAS
                if la in filepath.stem
            ]
        )
    )  # Using list(set()) as a hacky way of removing duplicate filepaths...


@task
def _load_sec_activity_by_month_sheet_inferring_headers(filepath: Path) -> pd.DataFrame:

    sheet_name = "SEC activity by month"

    # Find row containing column headers...
    header_row_not_found = True
    row_number = 1
    sheet = load_workbook(filepath)[sheet_name]

    while header_row_not_found:

        row = sheet[row_number]
        row_values = [cell.value for cell in row[:10]]
        # search first 10 columns...
        if "SEC Name" in row_values:
            header_row_not_found = False
            break

        # If not found in first 20 rows...
        if row_number == 20:
            raise ValueError(f"No header row found in {filepath}")

        row_number += 1

    # Load excel sheet using headers found above...
    return pd.read_excel(
        filepath, sheet_name=sheet_name, header=row_number - 1, engine="openpyxl"
    )


@task
def _drop_empty_rows(excel_df: pd.DataFrame,) -> pd.DataFrame:
    """Drops all empty rows in the second column - which is column 1 before 
    headers are set...  

    Parameters
    ----------
    excel_df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    return excel_df[excel_df.iloc[:, 1].notna()]


@task
def _get_rlpdt_totals(excel_df: pd.DataFrame,) -> pd.DataFrame:

    excel_df = excel_df.set_index("SEC Name")

    rlpdt_totals = ["Recruit", "Learn", "Plan", "Do", "Total"]

    return excel_df[rlpdt_totals]


@task
def _merge_mentor_dataframes(excel_dfs: List[pd.DataFrame]) -> pd.DataFrame:

    return pd.concat(excel_dfs)


@task
def _save_merged_dataframe_to_excel(
    merged_df: pd.DataFrame, savedir: Path, filename: str
) -> pd.DataFrame:

    merged_df.to_excel(savedir / filename)
