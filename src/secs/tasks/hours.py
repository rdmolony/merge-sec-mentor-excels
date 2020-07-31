from datetime import datetime
from typing import Tuple, List

import numpy as np
import pandas as pd
from prefect import task

from secs.tasks.utilities import replace_header_with_row


def _extract_sec_activity_hours_for_month(
    activities: pd.DataFrame, month: datetime
) -> pd.DataFrame:

    date_index = np.where(activities.values == month)

    column_index = int(date_index[1])
    from_column = column_index - 1  # County Mentor col occurs 1 before date
    to_column = column_index + 9  # Plan & Do for month is 10 columns

    all_hours = activities.copy().pipe(replace_header_with_row, header_row=7)

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


def _calculate_sec_activities_total(activities: pd.DataFrame) -> pd.DataFrame:

    return (
        activities.copy()
        .assign(mentor=lambda df: df["mentor"].fillna(df["local_authority"]))
        .assign(total=lambda df: df.sum(axis=1))
        .pivot_table(index="mentor", values=["total"])
        .reset_index()
    )


@task
def calculate_monthly_sec_activity_days(
    sec_activities: pd.DataFrame, month: datetime
) -> Tuple[pd.DataFrame]:

    monthly_hours = _extract_sec_activity_hours_for_month(sec_activities, month)
    planned, achieved = _split_sec_activity_hours_for_month(monthly_hours)

    planned_total = _calculate_sec_activities_total(planned)
    achieved_total = _calculate_sec_activities_total(achieved)

    import ipdb

    ipdb.set_trace()

    return {"planned": planned_total, "achieved": achieved_total}


def _extract_other_activity_hours_for_month(
    activities: pd.DataFrame, month: datetime
) -> pd.DataFrame:

    date_index = np.where(activities.values == month)

    column_index = int(date_index[1])
    from_column = column_index - 1  # County Mentor col occurs 1 before date
    to_column = column_index + 4  # Plan & Do for month is 5 columns

    all_hours = activities.copy().pipe(replace_header_with_row, header_row=7)

    return (
        all_hours.copy()
        .iloc[:, from_column:to_column]
        .assign(local_authority=all_hours["local_authority"])
        .convert_dtypes()
    )


def _split_other_activity_hours_for_month(
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


def _calculate_other_activities_total(activities: pd.DataFrame,) -> pd.DataFrame:

    return (
        activities.copy()
        .assign(mentor=lambda df: df["mentor"].fillna(df["local_authority"]))
        .pivot_table(index="mentor", values=["total"])
        .reset_index()
    )


@task
def calculate_monthly_other_activity_days(
    other_activities: pd.DataFrame, month: datetime,
) -> Tuple[pd.DataFrame]:

    monthly_hours = _extract_other_activity_hours_for_month(other_activities, month)
    planned, achieved = _split_other_activity_hours_for_month(monthly_hours)

    planned_total = _calculate_other_activities_total(planned)
    achieved_total = _calculate_other_activities_total(achieved)

    return {"planned": planned_total, "achieved": achieved_total}
