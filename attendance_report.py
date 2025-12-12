from datetime import date
from typing import Tuple
import pandas
from report_utils import (
    ATTENDANCE_EMAILS,
    MAYBE_STATES,
    SINGING_STATES,
    Email,
    _action_item,
    _fill_and_sort,
    _projected_absence_details,
    _table,
    _wrap_body,
    format_absence_totals,
    format_singers_indented,
    format_subtotals_table,
)


def generate_attendance_report(
    actual_attendance: pandas.DataFrame,
    projected_attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    current_nyc_date: date,
    cycle_from: date,
    cycle_to: date,
) -> Tuple[Email, bool]:
    """Returns: Email, worth_sending"""

    concerts = [c for c in roster.columns if isinstance(c, date)]

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

    actual_attendance["Attended"] = actual_attendance[past_rehearsals].sum(axis=1)
    actual_attendance["Absences"] = len(past_rehearsals) - actual_attendance.Attended

    # Only count explicitly marked absences, not dates that haven't been marked
    projected_attendance["Absences"] = (projected_attendance[future_rehearsals] == 0).sum(axis=1)

    join = roster.merge(projected_attendance, on="Name", how="outer", indicator="projected")
    join = join.merge(
        actual_attendance, on="Name", how="outer", indicator="actual", suffixes=("_projected", "_actual")
    )
    join = _fill_and_sort(join)

    join["Absences_total"] = join.Absences_actual + join.Absences_projected

    on_monday_roster = join.projected != "right_only"

    singing_this_cycle = join[cycle_to].isin(SINGING_STATES)
    maybe_this_cycle = join[cycle_to].isin(MAYBE_STATES)

    attended_at_least_one = join.Attended > 0

    other_cycles = join[concerts].drop(columns=cycle_to)
    other_cycles_yes = other_cycles.isin(SINGING_STATES).any(axis=1) & ~singing_this_cycle
    other_cycles_maybe = other_cycles.isin(MAYBE_STATES).any(axis=1) & ~(
        singing_this_cycle | other_cycles_yes
    )
    gone = on_monday_roster & ~(singing_this_cycle | other_cycles_yes | other_cycles_maybe)

    active_emails = join["Chorus Emails"] == "Yes"

    relevant_absences = singing_this_cycle & (join.Absences_total >= 3)

    if most_recent_rehearsal is not None:
        present_tonight = (join[f"{most_recent_rehearsal}_actual"] == 1).fillna(False)
        absent_tonight = singing_this_cycle & ~present_tonight
        marked_absent = join[f"{most_recent_rehearsal}_projected"] == 0
    else:
        present_tonight = absent_tonight = marked_absent = pandas.Series(False, index=join.index)

    join["Excused"] = marked_absent.fillna(False).map(lambda x: "Marked in CG" if x else "Unexcused?")
    if future_rehearsals:
        next_rehearsal = min(future_rehearsals)
        next_week = _projected_absence_details(join[singing_this_cycle], next_rehearsal)
    else:
        next_rehearsal = "N/A"
        next_week = "<p>No more rehearsals this cycle!</p>"

    subtotals = {
        "Present": present_tonight,
        "Absent": absent_tonight,
    }

    body = f"""
    <h1>This Week ({most_recent_rehearsal})</h1>
    {format_subtotals_table(join[singing_this_cycle], subtotals)}

    <h2>Absence details:</h2>
    {_table(join[absent_tonight], columns=["Name", "Excused", "Voice Part"])}

    <p>{_action_item("<b>Maggie/Attendance</b>: please confirm that folks listed above were truly absent, and/or whether they told us in advance. "
                     "<b>Janara</b>: once Maggie has confirmed, please make any necessary corrections to today's attendance in ChoirGenius, "
                     "and follow up with those who were AWOL.")}</p>

    <br><hr>

    <h1>Next Rehearsal ({next_rehearsal})</h1>
    {next_week}

    <br><hr>

    <h1>This Cycle ({first_rehearsal} to {cycle_to})</h1>

    <h2><b>{singing_this_cycle.sum()}</b> singers have said they'll participate:</h2>
    {format_subtotals_table(join[singing_this_cycle], {f"{cycle_to.strftime(r'%B')} Roster": singing_this_cycle})}

    <h2>Plus, <b>{maybe_this_cycle.sum()}</b> others are still listed as "maybe":</h2>
    {format_singers_indented(join[maybe_this_cycle])}
    <p>"Maybes" do not count toward the Roster stats above, nor to the Absence Totals below.</p>
    <p>
    {_action_item('<b>Janara</b>: please confirm their intentions, and move them to "Yes" or "No" ASAP.')}
    </p>

    <h2>Absence Totals:</h2>
    {format_absence_totals(join[relevant_absences])}
    <p>Singers with 3 or more absences are subject to make-up sessions, or being asked to sit out.</p>
    <p>
    {_action_item('<b>Section Leaders</b>: please determine the musical needs of the affected singers, '
                  'and make the necessary arrangements with Mark (to assess preparedness) or Janara '
                  '(to remove them from the current cycle).')}
    </p>

    <br><hr>

    <h1>Looking Ahead</h1>

    <h2><b>{(other_cycles_yes | other_cycles_maybe).sum()}</b> singers are sitting out this cycle:</h2>
    <ul>
        <li>
            <p><b>{other_cycles_yes.sum()}</b> said they'd be back later this season:</p>
            {format_singers_indented(join[other_cycles_yes])}
        </li>
        <li>
            <p><b>{other_cycles_maybe.sum()}</b> said "maybe":</p>
            {format_singers_indented(join[other_cycles_maybe])}
        </li>
    </ul>

    <h2>The rest ({gone.sum()}) are not expected back this season:</h2>
    {format_singers_indented(join[gone])}

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
