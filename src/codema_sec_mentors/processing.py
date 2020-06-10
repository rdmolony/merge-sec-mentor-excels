from pathlib import Path

import pandas as pd
from prefect import Flow, task
from pipeop import pipes

from _filepaths import DATA_DIR, MENTOR_EXCEL_FILEPATHS
from _globals import MENTOR_LAS
from utilities.flow import run_flow


# ETL Flow
# ********


@pipes
def process_mentor_excels() -> Flow:

    with Flow("Process SEC mentor excels") as flow:

        hourly_overview = (
            _load_excel_sheet(MENTOR_EXCEL_FILEPATHS[0], "SEC activity by month")
            >> _drop_rows_before_headers
            >> _drop_empty_rows
            >> _set_first_row_as_column_headers
            >> _drop_rlpdt_totals
            >> _pivot_secs_to_columns
            >> _calculate_rlpdt_hours
        )

    return flow


# Tasks
# *****


@task
def _load_excel_sheet(filepath: Path, sheet_name: str) -> pd.DataFrame:

    return pd.read_excel(filepath, sheet_name=sheet_name, header=None)


@task
def _initialise_empty_df() -> pd.DataFrame:

    return pd.DataFrame()


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

    index_of_header_row = excel_df[excel_df.eq("SEC Name").any(1)].index[0]

    return excel_df.iloc[index_of_header_row:]


@task
def _drop_empty_rows(excel_df: pd.DataFrame,) -> pd.DataFrame:

    with_no_empty_rows_in_first_three_columns = (
        excel_df[excel_df.columns[:3]].notna().any(axis="columns")
    )
    return excel_df[with_no_empty_rows_in_first_three_columns]


@task
def _set_first_row_as_column_headers(excel_df: pd.DataFrame,) -> pd.DataFrame:

    excel_df.columns = excel_df.iloc[0]

    return excel_df.drop(excel_df.index[0])


@task
def _drop_rlpdt_totals(excel_df: pd.DataFrame,) -> pd.DataFrame:
    """Excel's calculated totals for Research, Learn, Plan, Do, Total need to 
    beremoved to enable recalculation of the totals in Python.  
    
    This function removes the first column occurence of each which is the 
    location of these totals... 

    Parameters
    ----------
    excel_df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    rlpd_columns = ["Recruit", "Learn", "Plan", "Do", "Total"]
    excel_df_columns = excel_df.columns.tolist()
    indexes_of_first_occurences = []

    for column in rlpd_columns:
        indexes_of_first_occurences.append(excel_df_columns.index(column))

    return excel_df.drop(excel_df.columns[indexes_of_first_occurences], axis=1)


@task
def _pivot_secs_to_columns(excel_df: pd.DataFrame,) -> pd.DataFrame:

    return excel_df.pivot(columns="SEC Name").stack(level=0).droplevel(0)


@task
def _calculate_rlpdt_hours(excel_df: pd.DataFrame,) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()

    rlpdt = ["Recruit", "Learn", "Plan", "Do", "Total"]


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
