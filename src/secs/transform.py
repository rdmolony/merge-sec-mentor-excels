from collections import defaultdict
from typing import Dict, List

import icontract
import numpy as np
import pandas as pd
import prefect
from prefect import task


def _replace_header_with_row(df: pd.DataFrame, header_row: int) -> pd.DataFrame:

    df = df.copy()

    # Convert Excel row number into equiv pandas row number
    # (i.e. zero indexed and skip one row for header)
    header_row -= 2
    new_first_row = header_row + 1

    df.columns = df.iloc[header_row]
    df = df.iloc[new_first_row:].reset_index(drop=True)
    df.columns.name = ""

    return df


def _rename_columns_to_unique_names(df: pd.DataFrame) -> pd.DataFrame:

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


def _fillna_to_zero_in_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    numeric_columns = df.convert_dtypes().select_dtypes(np.number)
    df.loc[:, numeric_columns.columns] = numeric_columns.fillna(0)
    return df


def _drop_rows_where_first_column_empty(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    first_column = df.columns[0]
    df = df.dropna(subset=[first_column])

    return df


@task
@icontract.ensure(lambda result: not result.empty, "Output DataFrame must not be empty")
def transform_sheet(
    excel_sheets_raw: List[pd.DataFrame], header_row: int,
) -> pd.DataFrame:

    excel_sheets_clean = [
        df.copy()
        .pipe(_replace_header_with_row, header_row)
        .pipe(_rename_columns_to_unique_names)
        .replace("?", np.nan)
        .replace(" ", np.nan)
        .replace(0, np.nan)
        .pipe(_drop_rows_where_first_column_empty)
        # .pipe(_fillna_to_zero_in_numeric_columns)
        for df in excel_sheets_raw
    ]

    df = pd.concat(excel_sheets_clean).reset_index(drop=True)

    import ipdb

    ipdb.set_trace()

    return df
