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
            _drop_rows_before_headers.map(hourly_overview_raw)
            >> _drop_empty_rows.map()
            >> _set_first_row_as_column_headers.map()
            >> _rename_duplicate_column_names.map()
            >> _get_rlpdt_totals.map()
        )

        _save_totals_to_excel(hourly_overview_clean, RESULTS_DIR, filepaths)

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
def _load_sec_activity_by_month_sheet(filepath: Path) -> pd.DataFrame:

    sheet_name = "SEC activity by month"

    # NOTE: this function fails for some LAs as the headers are
    # typically 6 or 7 rows deep
    return pd.read_excel(
        filepath, sheet_name=sheet_name, header=None, engine="openpyxl",
    )


@task
def _load_sec_activity_by_month_sheet_inferring_headers(filepath: Path) -> pd.DataFrame:

    sheet_name = "SEC activity by month"

    # Find row containing column headers...
    header_row_not_found = True
    row_number = 1
    sheet = load_workbook(filepath)[sheet_name]

    while header_row_not_found:

        row = sheet[row_number]

        # search first 10 columns...
        if "SEC Name" in (cell.value for cell in row[:10]):
            header_row_not_found = False

        # If not found in first 20 rows...
        if row_number == 20:
            raise ValueError(f"No header row found in {filepath}")

        row_number += 1

    # Load excel sheet using headers found above...
    return pd.read_excel(
        filepath, sheet_name=sheet_name, header=row_number, engine="openpyxl"
    )


@task
def _drop_rows_based_on_na_values_in_first_column(
    excel_df: pd.DataFrame,
) -> pd.DataFrame:
    """As the column headers are typically in rows 6 or 7 need to drop
    all rows prior to these headers.  This will fail if values in column A
    are non-empty prior to headers...

    Parameters
    ----------
    raw_excel_df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    return excel_df[excel_df[0].notna()]


@task
def _drop_rows_before_headers(excel_df: pd.DataFrame,) -> pd.DataFrame:
    """As the column headers are typically in rows 6 or 7 need to drop
    all rows prior to these headers.  This function looks for the first 
    occurance of a row containing 'SEC Name' and removes the rows above it

    Parameters
    ----------
    raw_excel_df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    import ipdb

    ipdb.set_trace()

    index_of_header_row = excel_df[excel_df.eq("SEC Name").any(1)].index[0]

    return excel_df.iloc[index_of_header_row:]


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

    return excel_df[excel_df[1].notna()]


@task
def _set_first_row_as_column_headers(excel_df: pd.DataFrame,) -> pd.DataFrame:

    excel_df.columns = excel_df.iloc[0]

    return excel_df.drop(excel_df.index[0])


@task
def _rename_duplicate_column_names(excel_df: pd.DataFrame,) -> pd.DataFrame:

    # NOTE: https://stackoverflow.com/questions/40774787/renaming-columns-in-a-pandas-dataframe-with-duplicate-column-names

    renamer = defaultdict()

    for col in excel_df.columns[excel_df.columns.duplicated(keep=False)].tolist():
        if col not in renamer:
            renamer[col] = [col + "_0"]
        else:
            renamer[col].append(col + "_" + str(len(renamer[col])))

    return excel_df.rename(columns=lambda c: renamer[c].pop(0) if c in renamer else c)


@task
def _get_rlpdt_totals(excel_df: pd.DataFrame,) -> pd.DataFrame:

    excel_df = excel_df.set_index("SEC Name")

    rlpdt_totals = ["Recruit_0", "Learn_0", "Plan_0", "Do_0", "Total"]

    return excel_df[rlpdt_totals]


@task
def _save_totals_to_excel(
    excel_df: pd.DataFrame, savedir: Path, filepath: Path
) -> None:

    excel_df.to_excel(savedir / f"{filepath.stem}.xlsx")


# Roughwork
"""
column_names = [
    "SEC Name",
    "County / LA",
    "Recruit",
    "Learn",
    "Plan",
    "Do",
    "L-P-D",
    "EMP application",
]
"""
