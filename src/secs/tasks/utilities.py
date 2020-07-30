import re
from collections import defaultdict
from datetime import datetime, date
from logging import Logger
from pathlib import Path
from re import VERBOSE
from shutil import copyfile
from typing import Dict, List

import pandas as pd
import prefect
from prefect import task

from secs.tasks.extract import read_excel_to_dict


@task
def create_master_excel(template_filepath: Path, master_filepath: Path) -> None:

    logger = prefect.context.get("logger")
    if master_filepath.exists():
        logger.info(f"Skip copying as {master_filepath} already exists ...")
    else:
        logger.info(f"Copying {template_filepath} to {master_filepath} ...")
        copyfile(template_filepath, master_filepath)


def dataframe_contains_invalid_references(df: pd.DataFrame) -> bool:

    df = df.copy()

    return any(bug in df.values for bug in ["#REF!", "#VALUE!"])


@task
def raise_excels_with_invalid_references_in_sheets(
    excel_filepath: Path, sheet_names: List[str],
) -> None:

    logger = prefect.context.get("logger")
    sheets = read_excel_to_dict.run(excel_filepath)

    for sheet_name in sheet_names:
        if dataframe_contains_invalid_references(sheets[sheet_name]):
            logger.error(
                f"\n\n{sheet_name} in {excel_filepath} contains invalid references!\n\n"
            )


def replace_header_with_row(df: pd.DataFrame, header_row: int) -> pd.DataFrame:

    df = df.copy()

    # Convert Excel row number into equiv pandas row number
    # (i.e. zero indexed and skip one row for header)
    header_row -= 2
    new_first_row = header_row + 1

    df.columns = df.iloc[header_row]
    df = df.iloc[new_first_row:].reset_index(drop=True)
    df.columns.name = ""

    return df


def rename_columns_to_unique_names(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    renamer = defaultdict()

    for col in df.columns[df.columns.duplicated(keep=False)].tolist():
        if col not in renamer:
            renamer[col] = [col + "_0"]
        else:
            renamer[col].append(col + "_" + str(len(renamer[col])))

    return df.rename(
        columns=lambda column_name: renamer[column_name].pop(0)
        if column_name in renamer
        else column_name
    )


@task
def get_local_authority_from_filepath(filepath: Path) -> str:

    return re.findall(r"SEC - CM - (\w+).xlsx", str(filepath))[0]


def get_datetime_from_abbreviated_month(month_name: str) -> datetime:
    """Translates abbreviated 3-letter month string such as Jan for January etc
    into a Python datetime object.  This function assumes that the desired
    date occurs in the current year.
    
    Example
    -------
    get_datetime_from_abbreviated_month('Jul')
    >> datetime.datetime(2020, 7, 1, 0, 0)

    Parameters
    ----------
    month_name : str
        The month name; must be abbreviated!

    Returns
    -------
    datetime
        A datetime object corresponding to the first day of the month
    """
    year = date.today().strftime("%Y")
    return datetime.strptime(f"{month_name} {year}", "%b %Y")
