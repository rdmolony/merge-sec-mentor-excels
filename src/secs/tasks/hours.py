from datetime import datetime

import numpy as np
import pandas as pd

from secs.tasks.utilities import replace_header_with_row


def _extract_monthly_hours(activities: pd.DataFrame, month: datetime) -> pd.DataFrame:

    date_index = np.where(activities.values == month)

    column_index = int(date_index[1])
    from_column = column_index - 1  # County Mentor col occurs 1 before date
    to_column = column_index + 9  # Plan & Do for month is 10 columns

    all_hours = activities.copy().pipe(replace_header_with_row, header_row=7)

    return (
        all_hours.copy()
        .iloc[:, from_column:to_column]
        .assign(local_authority=all_hours["local_authority"])
    )


# def _split_monthly_hours(mo)


@task
def calculate_monthly_sec_activity_days(
    sec_activities: pd.DataFrame, month: datetime
) -> pd.DataFrame:

    monthly_hours = _extract_monthly_hours(sec_activities, month)

    # plan =


def calculate_monthly_other_activity_days(
    other_activities: pd.DataFrame,
) -> pd.DataFrame:
    pass

