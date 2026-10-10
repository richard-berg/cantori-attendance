from datetime import date
from typing import Tuple
import pandas
from choirgenius import EVENT_WEIGHTS
from report_utils import (
    ATTENDANCE_EMAILS,
    MAYBE_STATES,
    SINGING_STATES,
    Email,
    _action_item,
    _fill_and_sort,
    _table,
    _wrap_body,
    emphasize_absences,
    format_absence_totals,
    format_singers_indented,
    format_subtotals_table,
    weighted_absences,
)


def _owner(name: str) -> str:
    return f'<b style="background-color: #FDFD96; padding: 0 0.2rem;">{name}</b>'


# Singer's Handbook attendance thresholds -> follow-up action
ABSENCE_THRESHOLDS = {
    2: f"{_owner('Section Leaders')} check in verbally; seat them next to you.",
    3: f"{_owner('Section Leaders')} email about their attendance &amp; how we can support them.",
    4: f"{_owner('Mark')} checks their preparedness; they may be asked to withdraw.",
    5: f"{_owner('Janara')} removes them from this cycle (no exceptions).",
}
ORDINALS = {2: "2nd", 3: "3rd", 4: "4th", 5: "5th+"}


def _ordinal(n: int) -> str:
    suffix = "th" if 11 <= n % 100 <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _format_absence_number(absences: float) -> str:
    """Which absence this was for the singer; 1st is left blank."""
    if pandas.isna(absences) or absences < 2:
        return ""
    return emphasize_absences(int(absences), _ordinal(int(absences)))


def _format_absence_checklist(absent: pandas.DataFrame) -> str:
    if absent.empty:
        return ""

    awol = " Follow up with AWOL singers." if (absent.Excused == "AWOL?").any() else ""
    lines = [
        f"{_owner('Stephen/Attendance')}: confirm who was truly absent, and who told us in advance.",
        f"{_owner('Janara')}: then fix tonight's attendance in ChoirGenius.{awol}",
    ]
    tiers = set(absent.Absences_actual.dropna().clip(upper=5).astype(int))
    lines += [
        f"<b>{ORDINALS[n]} absence</b>: {action}" for n, action in ABSENCE_THRESHOLDS.items() if n in tiers
    ]

    items = "\n".join(f'<li style="margin-bottom: 0.5rem">{line}</li>' for line in lines)
    return f"<ul>{items}</ul>"


def generate_attendance_report(
    actual_attendance: pandas.DataFrame,
    projected_attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    current_nyc_date: date,
    cycle_from: date,
    cycle_to: date,
) -> Tuple[Email, bool]:
    """Returns: Email, worth_sending"""

    past_rehearsals = [c for c in actual_attendance.columns if isinstance(c, date)]
    first_rehearsal = min(past_rehearsals) if past_rehearsals else cycle_from
    most_recent_rehearsal = max(past_rehearsals) if past_rehearsals else None
    today_is_thursday = current_nyc_date.weekday == 3
    worth_sending = today_is_thursday or most_recent_rehearsal == current_nyc_date

    future_rehearsals = [
        c
        for c in projected_attendance.columns
        if isinstance(c, date) and (most_recent_rehearsal is None or c > most_recent_rehearsal)
    ]

    actual_attendance["Attended"] = (actual_attendance[past_rehearsals] > 0).sum(axis=1)
    actual_attendance["Absences"] = weighted_absences(
        actual_attendance[past_rehearsals].fillna(0), actual_attendance.attrs.get(EVENT_WEIGHTS, {})
    )
    actual_attendance["Partials"] = (actual_attendance[past_rehearsals] == 0.5).sum(axis=1)

    # Only count explicitly marked absences, not dates that haven't been marked
    projected_attendance["Absences"] = weighted_absences(
        projected_attendance[future_rehearsals], projected_attendance.attrs.get(EVENT_WEIGHTS, {})
    )

    join = roster.merge(projected_attendance, on="Name", how="outer", indicator="projected")
    join = join.merge(
        actual_attendance, on="Name", how="outer", indicator="actual", suffixes=("_projected", "_actual")
    )
    join = _fill_and_sort(join)

    join["Absences_total"] = join.Absences_actual + join.Absences_projected

    singing_this_cycle = join[cycle_to].isin(SINGING_STATES)
    maybe_this_cycle = join[cycle_to].isin(MAYBE_STATES)

    attended_at_least_one = join.Attended > 0

    active_emails = join["Chorus Emails"] == "Yes"

    at_risk = singing_this_cycle & (join.Absences_total >= 3)

    if most_recent_rehearsal is not None:
        present_tonight = (join[f"{most_recent_rehearsal}_actual"] > 0).fillna(False)
        partial_tonight = (join[f"{most_recent_rehearsal}_actual"] == 0.5).fillna(False)
        absent_tonight = singing_this_cycle & ~present_tonight
        marked_absent = join[f"{most_recent_rehearsal}_projected"] == 0
    else:
        present_tonight = partial_tonight = absent_tonight = marked_absent = pandas.Series(
            False, index=join.index
        )

    join["Excused"] = marked_absent.fillna(False).map(lambda x: "Marked" if x else "AWOL?")
    join["Absence_number"] = join.Absences_actual.map(_format_absence_number)

    subtotals = {
        "Present": present_tonight,
        "Absent": absent_tonight,
        "Late": partial_tonight,
    }

    maybe_section = ""
    if maybe_this_cycle.any():
        maybe_section = f"""
    <h2>Plus, <b>{maybe_this_cycle.sum()}</b> others are still listed as "maybe":</h2>
    {format_singers_indented(join[maybe_this_cycle])}
    <p>"Maybes" do not count toward the Roster stats above, nor to the At Risk stats below.</p>
    <p>
    {_action_item('<b>Janara</b>: please confirm their intentions, and move them to "Yes" or "No" ASAP.')}
    </p>
    """

    body = f"""
    <h1>This Week ({most_recent_rehearsal})</h1>
    {format_subtotals_table(join[singing_this_cycle], subtotals)}

    <h2>Absence details:</h2>
    {_table(join[absent_tonight], columns=["Name", "Excused", "Absence_number"], color_names=True)}
    {_format_absence_checklist(join[absent_tonight])}

    <h2>Singers who were late:</h2>
    {format_singers_indented(join[singing_this_cycle & partial_tonight])}

    <br><hr>

    <h1>This Cycle ({first_rehearsal} to {cycle_to})</h1>

    <h2><b>{singing_this_cycle.sum()}</b> singers have said they'll participate:</h2>
    {format_subtotals_table(
        join[singing_this_cycle],
        {f"{cycle_to.strftime(r'%B')}<br>Roster": singing_this_cycle, "At<br>Risk": at_risk},
    )}

    {maybe_section}

    <h2>At risk (3+ projected absences):</h2>
    {format_absence_totals(join[at_risk])}

    <br><hr>

    <h1>Consistency Checks</h1>

    <p>{_action_item("<b>Janara/Attendance</b>: please double-check that these make sense")}
    (i.e. aren't the result of inconsistent data entry in ChoirGenius vs Monday.com)</p>

    <ul>

    <li>
    <p>Currently getting chorus emails, yet aren't on the concert roster for this cycle:</p>
    {format_singers_indented(join[active_emails & ~singing_this_cycle])}
    </li>

    <li>
    <p>On the current concert roster, yet aren't getting emails:</p>
    {format_singers_indented(join[singing_this_cycle & ~active_emails])}
    </li>

    <li>
    <p>Have attended rehearsal(s) this cycle, yet aren't on the current concert roster:</p>
    {format_singers_indented(join[attended_at_least_one & ~singing_this_cycle])}
    </li>

    </ul>
    """

    email = Email(
        subject=f"Attendance Report for {most_recent_rehearsal}",
        body=_wrap_body(body),
        to=ATTENDANCE_EMAILS,
    )
    return email, worth_sending
