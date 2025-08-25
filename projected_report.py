from datetime import date
from typing import Tuple
import pandas
from report_utils import (
    ATTENDANCE_EMAILS,
    SINGING_STATES,
    Email,
    _fill_and_sort,
    _projected_absence_details,
    _wrap_body,
    format_subtotals_table,
)


def generate_projected_attendance_report(
    projected_attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    today: date,
    cycle_to: date,
) -> Tuple[Email, bool]:
    """Returns: Email, worth_sending"""

    join = roster.merge(projected_attendance, on="Name", how="outer", indicator="projected")
    join = _fill_and_sort(join)

    next_rehearsal = min(c for c in projected_attendance.columns if isinstance(c, date) and c >= today)
    worth_sending = next_rehearsal == today

    singing_this_cycle = join[cycle_to].isin(SINGING_STATES)

    confirmed = join[next_rehearsal] == 1
    marked_absent = join[next_rehearsal] == 0
    not_marked = join[next_rehearsal].isna()

    subtotals = {
        "Confirmed": confirmed,
        "Marked Absent": marked_absent,
        "Not Marked": not_marked,
    }

    body = f"""
    <h1>Rehearsal Roster for {next_rehearsal}</h1>
    {format_subtotals_table(join[singing_this_cycle], subtotals)}

    <h1>Absence Details</h1>
    {_projected_absence_details(join[singing_this_cycle], next_rehearsal)}
    """  # noqa: F821

    email = Email(
        subject=f"Projected attendance for {next_rehearsal}", body=_wrap_body(body), to=ATTENDANCE_EMAILS
    )
    return email, worth_sending
