from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import prefect
from prefect import task

from secs.tasks.utilities import replace_header_with_row


def _extract_sec_activity_hours_for_month(
    activities: pd.DataFrame, month: datetime
) -> pd.DataFrame:

    date_index = np.where(activities.values == month)

    column_index = int(date_index[1][0])
    from_column = column_index - 1  # County Mentor col occurs 1 before date
    to_column = column_index + 9  # Plan & Do for month is 10 columns

    all_hours = activities.copy().pipe(replace_header_with_row, header_row=8)

    return (
        all_hours.copy()
        .iloc[:, from_column:to_column]
        .assign(local_authority=all_hours["local_authority"])
        .convert_dtypes()
    )


def _rename_columns_to(df: pd.DataFrame, new_names=List[str]) -> pd.DataFrame:

    df.columns = new_names

    return df


def _split_sec_activity_hours_for_month(
    activities_for_month: pd.DataFrame,
) -> Tuple[pd.DataFrame]:

    planned = (
        activities_for_month.copy()
        .set_index("local_authority")
        .iloc[:, :5]
        .reset_index()
        .pipe(
            _rename_columns_to,
            ["local_authority", "mentor", "recruit", "learn", "plan", "do"],
        )
    )

    achieved = (
        activities_for_month.copy()
        .set_index("local_authority")
        .iloc[:, 5:]
        .reset_index()
        .pipe(
            _rename_columns_to,
            ["local_authority", "mentor", "recruit", "learn", "plan", "do"],
        )
    )

    return planned, achieved


@task
def get_sec_activity_days_for_month(
    sec_activities: pd.DataFrame, month: datetime
) -> Tuple[pd.DataFrame]:

    monthly_hours = _extract_sec_activity_hours_for_month(sec_activities, month)
    planned, achieved = _split_sec_activity_hours_for_month(monthly_hours)

    return {"planned": planned, "achieved": achieved}


@task
def calculate_sec_activity_totals_by_mentor(activities: pd.DataFrame) -> pd.DataFrame:

    return (
        activities.copy()
        .assign(mentor=lambda df: df["mentor"].fillna(df["local_authority"]))
        .assign(total=lambda df: df.sum(axis=1))
        .pivot_table(index="mentor", values=["total"])
        .reset_index()
    )


def _extract_misc_activity_hours_for_month(
    activities: pd.DataFrame, month: datetime
) -> pd.DataFrame:

    date_index = np.where(activities.values == month)

    column_index = int(date_index[1])
    from_column = column_index - 1  # County Mentor col occurs 1 before date
    to_column = column_index + 4  # Plan & Do for month is 5 columns

    all_hours = activities.copy().pipe(replace_header_with_row, header_row=8)

    return (
        all_hours.copy()
        .iloc[:, from_column:to_column]
        .assign(local_authority=all_hours["local_authority"])
        .convert_dtypes()
    )


def _split_misc_activity_hours_for_month(
    activities_for_month: pd.DataFrame,
) -> Tuple[pd.DataFrame]:

    planned = (
        activities_for_month.copy()
        .pipe(
            _rename_columns_to,
            [
                "mentor",
                "description",
                "planned",
                "mentor",
                "achieved",
                "local_authority",
            ],
        )
        .set_index(["local_authority", "description"])
        .iloc[:, :2]
        .reset_index()
        .pipe(_rename_columns_to, ["local_authority", "description", "mentor", "total"])
    )

    achieved = (
        activities_for_month.copy()
        .pipe(
            _rename_columns_to,
            [
                "mentor",
                "description",
                "planned",
                "mentor",
                "achieved",
                "local_authority",
            ],
        )
        .set_index(["local_authority", "description"])
        .iloc[:, 2:]
        .reset_index()
        .pipe(_rename_columns_to, ["local_authority", "description", "mentor", "total"])
    )

    return planned, achieved


@task
def get_misc_activity_days_for_month(
    misc_activities: pd.DataFrame, month: datetime,
) -> Tuple[pd.DataFrame]:

    monthly_hours = _extract_misc_activity_hours_for_month(misc_activities, month)
    planned, achieved = _split_misc_activity_hours_for_month(monthly_hours)

    return {"planned": planned, "achieved": achieved}


@task
def calculate_misc_activity_totals_by_mentor(activities: pd.DataFrame,) -> pd.DataFrame:

    return (
        activities.copy()
        .assign(mentor=lambda df: df["mentor"].fillna(df["local_authority"]))
        .convert_dtypes()
        .pivot_table(index="mentor", values=["total"])
        .reset_index()
    )


@task
def merge_planned_and_achieved_totals(
    sec_hours: Tuple[pd.DataFrame], misc_hours: Tuple[pd.DataFrame]
) -> Dict[str, pd.DataFrame]:

    planned_total = pd.merge(
        left=sec_hours[0],
        right=misc_hours[0],
        on="mentor",
        how="outer",
        suffixes=(" sec activities", " misc activities"),
    )
    achieved_total = pd.merge(
        left=sec_hours[1],
        right=misc_hours[1],
        on="mentor",
        how="outer",
        suffixes=(" sec activities", " misc activities"),
    )

    return {"planned": planned_total, "achieved": achieved_total}


def _extract_sec_hourly_total(sec_activity_sheet: pd.DataFrame,) -> None:

    all_hours = sec_activity_sheet.copy().pipe(replace_header_with_row, header_row=8)
    return (
        all_hours.copy()
        .set_index(["local_authority", "SEC Name"])
        .iloc[:, 10:15]
        .reset_index()
    )


@task
def check_if_too_many_planned_sec_hours(
    sec_activity_sheet: pd.DataFrame, monthly_totals: pd.DataFrame,
) -> None:

    logger = prefect.context.get("logger")

    import ipdb

    ipdb.set_trace()

    sec_hourly_total = _extract_sec_hourly_total(sec_activity_sheet)

    planned = monthly_totals["planned"]


def _extract_misc_hourly_total(sec_activity_sheet: pd.DataFrame,) -> None:

    all_hours = sec_activity_sheet.copy().pipe(replace_header_with_row, header_row=8)
    return (
        all_hours.copy()
        .set_index(["local_authority", "SEC Name"])
        .iloc[:, 10:15]
        .reset_index()
    )


@task
def check_if_too_many_planned_misc_hours(
    misc_activity_sheet: pd.DataFrame, monthly_totals: pd.DataFrame,
) -> None:

    logger = prefect.context.get("logger")

    pass

    # all_hours = misc_activity_sheet.copy().pipe(replace_header_with_row, header_row=8)
