from collections import defaultdict
from logging import Logger
from typing import Dict, List
from re import VERBOSE

import icontract
import numpy as np
import pandas as pd
import prefect
from prefect import task

from secs.tasks.utilities import (
    dataframe_contains_invalid_references,
    replace_header_with_row,
    rename_columns_to_unique_names,
)


def _select_numeric_columns(df: pd.DataFrame, logger: Logger = None) -> List[str]:

    column_names_numeric = []
    for column_name in df.columns:

        column = df[column_name].copy()

        numeric_rows = """
        ^                   # beginning of string
        (?:[^A-Za-z]+ )?    # (optional) not preceded by a word
        (?:[*,])?           # (optional) preceded by * or *
        (\d+)               # capture the digits
        (?:[%])?            # (optional) followed by %
        (?: [^A-Za-z]+)?    # (optional) not followed by a word
        $                   # end of string
        """
        any_row_contains_a_valid_number = (
            column.astype(str).str.contains(numeric_rows, flags=VERBOSE).any()
        )
        if any_row_contains_a_valid_number:
            column_names_numeric.append(column_name)

    if logger:

        logger.debug(f"\n\nNumeric column names:\n{column_names_numeric}")
        column_names_non_numeric = np.setdiff1d(
            df.columns.to_list(), column_names_numeric
        )
        logger.debug(f"\nNon-numeric column names:\n{column_names_non_numeric}")

    return column_names_numeric


def _clean_numeric_columns(df: pd.DataFrame, logger: Logger = None) -> pd.DataFrame:

    column_names_numeric = _select_numeric_columns(df)
    df.loc[:, column_names_numeric] = (
        df[column_names_numeric]
        .copy()
        .replace(
            r"\s+", np.nan, regex=True
        )  # remove whitespace so can convert str to float
        .replace(
            to_replace=r"[^1-9.]", value="", regex=True
        )  # remove non-numeric characters
        .replace(r"", np.nan, regex=True)
        .fillna(0)
        .astype(np.number)  # convert string columns to numbers
        .convert_dtypes()  # infer ints
    )

    return df


def _drop_rows_where_first_column_empty(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    first_column = df.columns[0]
    df = df.dropna(subset=[first_column])

    return df


@task
@icontract.ensure(lambda result: not result.empty, "Output cannot be empty!")
# @icontract.ensure(
#     lambda result: dataframe_contains_invalid_references(result),
#     "Output cannot contain invalid references!",
# )
def transform_sheet(
    excel_sheets_raw: List[pd.DataFrame], header_row: int,
) -> pd.DataFrame:

    logger = prefect.context.get("logger")

    excel_sheets_clean = [
        df.copy()
        .pipe(replace_header_with_row, header_row)
        .pipe(rename_columns_to_unique_names)
        .replace("?", np.nan)
        .replace(0, np.nan)
        .pipe(_clean_numeric_columns, logger)
        .pipe(_drop_rows_where_first_column_empty)
        for df in excel_sheets_raw
    ]

    df = pd.concat(excel_sheets_clean).reset_index(drop=True)

    return df
