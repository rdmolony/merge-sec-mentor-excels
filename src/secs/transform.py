from collections import defaultdict
from typing import Dict, List

import numpy as np
import pandas as pd
import prefect
from prefect import task


def _replace_header_with_first_row(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns.name = ""

    return df


def _rename_columns_to_unique_names(df: pd.DataFrame) -> pd.DataFrame:

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

    numeric_columns = df.convert_dtypes().select_dtypes(np.number)
    df.loc[:, numeric_columns.columns] = numeric_columns.fillna(0)
    return df


@task
def transform_sec_activity_by_month_sheet(
    sec_activity_by_month: List[pd.DataFrame],
) -> pd.DataFrame:

    sec_activity_by_month = [
        df.dropna(subset=["Unnamed: 1"]).pipe(_replace_header_with_first_row)
        for df in sec_activity_by_month
    ]

    return (
        pd.concat(sec_activity_by_month)
        .reset_index(drop=True)
        .replace(["?", " "], np.nan)
        .pipe(_rename_columns_to_unique_names)
        .pipe(_fillna_to_zero_in_numeric_columns)
    )


@task
def transform_other_activity_by_month_sheet(
    sec_activity_by_month: List[pd.DataFrame],
) -> pd.DataFrame:

    sec_activity_by_month = [
        df.dropna(subset=["Unnamed: 0"]).pipe(_replace_header_with_first_row)
        for df in sec_activity_by_month
    ]

    return (
        pd.concat(sec_activity_by_month)
        .reset_index(drop=True)
        .replace(["?", " "], np.nan)
        .pipe(_rename_columns_to_unique_names)
        .pipe(_fillna_to_zero_in_numeric_columns)
    )
