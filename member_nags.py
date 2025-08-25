from datetime import date
from typing import Iterable, List
import pandas

from report_utils import SINGING_STATES, Email, _fill_and_sort, _wrap_body


def generate_member_nags(
    projected_attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    cycle_to: date,
) -> Iterable[Email]:

    join = roster.merge(projected_attendance, on="Name", how="outer", indicator="projected")
    join = _fill_and_sort(join)

    singing_this_cycle = join[cycle_to].isin(SINGING_STATES)

    date_cols: List[date] = [c for c in join.columns if isinstance(c, date)]

    # Python doesn't have a day-of-month formatter w/o leading zero
    cycle_to_str = cycle_to.strftime("%B") + f" {cycle_to.day}"

    has_unmarked_rehearsals = join[singing_this_cycle & join[date_cols].isna().any(axis=1)]

    for _, row in has_unmarked_rehearsals.iterrows():
        first_name = row["Name"].split()[0]
        subject = f"{first_name}, please mark your Cantori attendance"
        body = f"""
        <h2>Dear {first_name},</h2>

        <p>
        Please <a href="https://cantori.choirgenius.com/calendar/events">mark your attendance plans</a> for the current Cantori cycle (through {cycle_to_str}).
        </p>

        <p>
        These dates are still unconfirmed:
        {format_unmarked_dates(row, date_cols)}
        </p>

        <p>
        Having everyone's plans up-to-date <a href="https://cantori.choirgenius.com/calendar/events">in ChoirGenius</a> is super helpful for rehearsal strategy.  Thank you!
        </p>
        """
        email = Email(
            subject=subject,
            body=_wrap_body(body),
            to=(row["Email"],),
            cc=("attendance@cantorinewyork.com", "richard.berg@cantorinewyork.com"),
        )
        yield email


def format_unmarked_dates(row: pandas.Series, date_cols: List[date]) -> str:
    unmarked_dates = sorted(d for d in date_cols if pandas.isna(row[d]))
    bullets = "\n".join(f"<li>{d}</li>" for d in unmarked_dates)
    return f"<ul>{bullets}</ul>"
