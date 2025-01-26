from datetime import date
from enum import Enum
from typing import List, Set, Tuple

import numpy as np
import pandas


class MondayConcertState(Enum):
    YES = "Yes"
    PARTIAL = "Partial"
    MAYBE = "Maybe"
    NO = "No"


SINGING_STATES = [MondayConcertState.YES.value, MondayConcertState.PARTIAL.value]
PARTIAL_STATES = [MondayConcertState.PARTIAL.value]
MAYBE_STATES = [MondayConcertState.MAYBE.value]
NO_STATES = [MondayConcertState.NO.value]


def generate_consistency_report(
    roster: pandas.DataFrame,
    candidates: pandas.DataFrame,
    cg_active: pandas.DataFrame,
    projected_concert_attendance: pandas.DataFrame,
    season: str,
    cycle_to: date,
) -> Tuple[str, str, bool]:
    """Returns: subject (txt), body (HTML)"""
    join = roster.merge(candidates, on="Name", how="outer", indicator="audition")
    join = join.merge(cg_active, left_on="Name", right_on="whole_name", how="outer", indicator="cg")

    concerts = [c for c in projected_concert_attendance.columns if isinstance(c, date)]
    projected_concert_attendance["Concerts_Marked_Absent"] = (
        projected_concert_attendance[concerts] == 0
    ).sum(axis=1)
    projected_concert_attendance["Concerts_Marked_Singing"] = (
        projected_concert_attendance[concerts] == 1
    ).sum(axis=1)
    join = join.merge(
        projected_concert_attendance[["Name", "Concerts_Marked_Absent", "Concerts_Marked_Singing"]],
        on="Name",
        how="outer",
        indicator="concert_attendance",
    )

    join = _fill_and_sort(join)

    accepted = (join.Group == season) & (join.RESULT == "Accepted")
    candidates_missing_from_roster = accepted & (join.audition == "right_only")

    not_in_cg = join.cg == "left_only"
    might_sing_this_cycle = join[cycle_to].isin(SINGING_STATES + MAYBE_STATES)
    roster_missing_from_cg = might_sing_this_cycle & not_in_cg

    cg_missing_from_both_monday_boards = join.cg == "right_only"
    cg_missing_from_roster_but_did_audition = (join.audition == "right_only") & (join.cg == "both")
    cg_missing_from_roster = cg_missing_from_both_monday_boards | cg_missing_from_roster_but_did_audition

    email_mismatch = format_mismatch_table(join, "Email", "primary_email")
    voice_mismatch = format_mismatch_table(join, "Voice Part", "voice_part")

    marked_no = join.Concerts_Marked_Absent == len(concerts)
    marked_partial = (join.Concerts_Marked_Absent > 0) & ~marked_no
    marked_yes = join.Concerts_Marked_Singing == len(concerts)
    marked_something = (join.Concerts_Marked_Absent + join.Concerts_Marked_Singing) > 0
    marked_no_but_might_sing = marked_no & might_sing_this_cycle
    marked_partial_but_not_partial = marked_partial & ~join[cycle_to].isin(PARTIAL_STATES)
    partial_roster_but_marked_nonpartial = (
        join[cycle_to].isin(PARTIAL_STATES) & ~marked_partial & marked_something
    )
    marked_yes_but_not_singing = marked_yes & join[cycle_to].isin(NO_STATES + MAYBE_STATES)

    worth_sending = any(
        [
            candidates_missing_from_roster.sum() > 0,
            cg_missing_from_roster.sum() > 0,
            roster_missing_from_cg.sum() > 0,
            email_mismatch != "None",
            voice_mismatch != "None",
            marked_no_but_might_sing.sum() > 0,
            marked_partial_but_not_partial.sum() > 0,
            marked_yes_but_not_singing.sum() > 0,
            partial_roster_but_marked_nonpartial.sum() > 0,
        ]
    )

    subject = "ChoirGenius vs Monday.com consistency check failed!"
    body = f"""
    <section>
    <h1>Monday.com Roster Issues</h1>
    <p>These singers:</p>
    <ul>
        <li>Cannot receive chorus emails</li>
        <li>Won't appear in Attendance reports, even if marked "present" in ChoirGenius</li>
    </ul>

    <h2>Audition status 'Accepted', but not in Roster</h2>
    {format_singers_indented(join[candidates_missing_from_roster])}

    <h2>ChoirGenius 'Active', but not in Roster</h2>
    {format_singers_indented(join[cg_missing_from_roster])}
    </section>

    <br><hr>

    <section>
    <h1>ChoirGenius Issues</h1>
    <p>These singers:</p>
    <ul>
        <li>Cannot be marked as Present at rehearsals</li>
        <li>Cannot download music or view the calendar</li>
    </ul>

    <h2>On the {cycle_to.strftime(r'%B')} roster*, but not 'Active' in CG</h2>
    {format_singers_indented(join[roster_missing_from_cg])}
    <p style="font-size: 0.75rem">*including "Maybes"</p>
    </section>

    <br><hr>

    <section>
    <h1>Concert Attendance Issues</h1>
    <p>These singers have explicitly marked themselves as Present/Absent for concert events in ChoirGenius,
    in a way that doesn't line up with our Monday roster.</p>

    <h2>Marked themself 'Absent' for all {cycle_to.strftime(r'%B')} concerts, but appear on the roster*</h2>
    {format_singers_indented(join[marked_no_but_might_sing])}

    <h2>Marked themself 'Absent' for <i>some</i> {cycle_to.strftime(r'%B')} concerts, but roster status is not 'Partial'</h2>
    {format_singers_indented(join[marked_partial_but_not_partial])}

    <h2>Marked themself 'Present' for all {cycle_to.strftime(r'%B')} concerts, but roster status is not 'Yes'</h2>
    {format_singers_indented(join[marked_yes_but_not_singing])}

    <h2>Roster status is 'Partial', but marked something else for {cycle_to.strftime(r'%B')} concerts</h2>
    {format_singers_indented(join[partial_roster_but_marked_nonpartial])}

    <p style="font-size: 0.75rem">*including "Maybes"</p>
    </section>

    <br><hr>

    <section>
    <h1>Data Mismatches</h1>
    <p>Depending which system is correct, these singers might:</p>
    <ul>
        <li>Want chorus emails sent to a different address</li>
        <li>Be on the wrong section email list</li>
        <li>Have everything functionally ok, but be confused by what they see in ChoirGenius</li>
    </ul>

    <h2>Email address mismatch</h2>
    {email_mismatch}

    <h2>Voice part mismatch</h2>
    {voice_mismatch}
    </section>
    """

    return subject, _wrap_body(body), worth_sending


def generate_projected_attendance_report(
    projected_attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    today: date,
    cycle_to: date,
) -> Tuple[str, str, bool]:
    """Returns: subject (txt), body (HTML)"""

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
    """

    subject = f"Projected attendance for {next_rehearsal}"
    return subject, _wrap_body(body), worth_sending


def generate_attendance_report(
    actual_attendance: pandas.DataFrame,
    projected_attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    current_nyc_date: date,
    cycle_to: date,
) -> Tuple[str, str, bool]:
    """Returns: subject (txt), body (HTML)"""

    concerts = [c for c in roster.columns if isinstance(c, date)]

    past_rehearsals = [c for c in actual_attendance.columns if isinstance(c, date)]
    first_rehearsal = min(past_rehearsals)
    most_recent_rehearsal = max(past_rehearsals)
    worth_sending = most_recent_rehearsal == current_nyc_date

    future_rehearsals = [
        c for c in projected_attendance.columns if isinstance(c, date) and c > most_recent_rehearsal
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

    present_tonight = (join[f"{most_recent_rehearsal}_actual"] == 1).fillna(False)
    absent_tonight = singing_this_cycle & ~present_tonight

    marked_absent = join[f"{most_recent_rehearsal}_projected"] == 0
    join["Excused"] = marked_absent.fillna(False).map(lambda x: "Marked in CG" if x else "Unexcused?")

    if future_rehearsals:
        next_rehearsal = min(future_rehearsals)
        next_week = _projected_absence_details(join[singing_this_cycle], next_rehearsal)
    else:
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

    <p>{_action_item("<b>Kim/Attendance</b>: please confirm that folks listed above were truly absent, and/or whether they told us in advance. "
                     "<b>Janara</b>: once Kim has confirmed, please make any necessary corrections to today's attendance in ChoirGenius, "
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

    subject = f"Attendance Report for {most_recent_rehearsal}"
    return subject, _wrap_body(body), worth_sending


def _action_item(msg: str) -> str:
    style = """
        font-size: 1.15rem;
        font-style: italic;
        background-color: #FDFD96;
    """
    return f'<span style="{style}">{msg}</span>'


def _projected_absence_details(
    df: pandas.DataFrame,
    rehearsal_col: str | date,
) -> str:
    return f"""
    <h2>Singers who are marked absent:</h2>
    {format_singers_indented(df[df[rehearsal_col] == 0])}

    <h2>Singers who haven't marked their plans in ChoirGenius:</h2>
    {format_singers_indented(df[df[rehearsal_col].isna()])}
    """


def format_mismatch_table(singers: pandas.DataFrame, monday_col: str, choirgenius_col: str) -> str:
    mismatch = (singers.cg == "both") & (singers[monday_col] != singers[choirgenius_col])

    df = singers[mismatch]
    if df.empty:
        return "None"

    df = df.rename(columns={monday_col: "Monday", choirgenius_col: "ChoirGenius"})
    return _table(df, columns=["Name", "Monday", "ChoirGenius"], headers=True)


def format_singers_oneline(singers: pandas.DataFrame) -> str:
    if singers.empty:
        return "None"

    def link_email_and_highlight_section(row: pandas.Series):
        style = f"""
            display: inline-block;
            text-decoration: none;
            color: white;
            background-color: {row["color"]};
            border: 1px;
            padding: 0.5rem 1rem;
        """
        return f'<a href="mailto:{row["Email"]}" style="{style}">{row["Name"]}</a>'

    htmls = singers.apply(link_email_and_highlight_section, axis=1)
    return "\n".join(htmls.to_list())


def format_singers_indented(singers: pandas.DataFrame) -> str:
    line = format_singers_oneline(singers)
    return f'<p style="margin-left: 1.5rem">{line}</p>'


def format_absence_totals(singers: pandas.DataFrame) -> str:
    df = singers.groupby(["Absences_total", "Absences_actual", "Absences_projected"]).agg({"Name": set})
    df = df.sort_index(ascending=False).reset_index()

    def format_name_aggregation(row: pandas.Series) -> str:
        aggregated_names: Set[str] = row["Name"]
        join = pandas.DataFrame(aggregated_names, columns=["Name"]).merge(singers, on="Name")
        join = join.sort_values(["sort_key", "Name"])
        return format_singers_oneline(join)

    df = df.assign(Names_Formatted=df.apply(format_name_aggregation, axis=1))
    col_names = {
        "Absences_total": "Total",
        "Absences_actual": "Actual",
        "Absences_projected": "Projected",
        "Names_Formatted": "Singers (click to email)",
    }
    df = df.rename(columns=col_names)
    return _table(df, columns=list(col_names.values()), headers=True)


def format_subtotals_table(df: pandas.DataFrame, indicators: dict[str, pandas.Series]) -> str:
    """Indicators: a map from column name -> boolean Series."""
    df = df.assign(**indicators)

    indicator_col_names = list(indicators.keys())
    groupby = ["Voice Part", "sort_key", "color"]
    all_cols = groupby + indicator_col_names
    display_cols = [c for c in all_cols if c not in ["sort_key", "color"]]

    df = df[all_cols].groupby(groupby).sum().reset_index().sort_values("sort_key")
    return _table(df, columns=display_cols, headers=True, totals=True)


def _fill_and_sort(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Fill: df has been joined against ChoirGenius, which may not be 1:1, so we
    want to fill in NAs where we can.

    Sort: always by SATB, then by Name.
    """
    replacements = {
        "color": df.color.fillna("black"),
        "sort_key": df.sort_key.fillna(float("inf")),
        "Email": df.Email.fillna("email-is-missing"),
    }

    if "whole_name" in df.columns:
        # We've been joined against a ChoirGenius member query (not just an
        # attendance query), so we have more data with which to fill in gaps
        replacements.update(
            {
                "Name": df.Name.fillna(df.whole_name),
                "Email": df.Email.fillna(df.EMAIL),
                "Voice Part": df["Voice Part"].fillna(df.voice_part),
            }
        )

    df = df.assign(**replacements)
    df = df.sort_values(["sort_key", "Name"])
    return df


def _table(df: pandas.DataFrame, columns: List[str], headers: bool = False, totals: bool = False) -> str:
    tbl_style = """
        border-collapse: collapse;
        border: 2px solid #eee;
    """

    col_map = {col: n for n, col in enumerate(df.columns)}

    rows: List[str] = []
    for i, row in enumerate(df.itertuples(index=False)):
        row_bg = "white" if i % 2 == 0 else "#eee"
        row_style = f"""
            background-color: {row_bg};
        """
        cells = ""
        for j, col in enumerate(columns):
            weight = "bold" if j == 0 else "normal"

            if col == "Name" and "Email" in col_map:
                cell_value = f"""
                <a href="mailto:{row[col_map["Email"]]}" style="text-decoration:none;">
                    {row[col_map["Name"]]}
                </a>
                """
            else:
                cell_value = row[col_map[col]]

            if col == "Voice Part" and "color" in col_map:
                cell_style = f"""
                    padding: 0.5rem 2rem;
                    color: white;
                    background-color: {row[col_map["color"]]};
                    text-align: center;
                    font-weight: {weight};
                """
            else:
                align = "center" if isinstance(cell_value, (int, np.integer)) else "left"
                cell_style = f"""
                    padding: 0.5rem 1rem;
                    color: black;
                    text-align: {align};
                    font-weight: {weight};
                """

            cells += f'\n<td style="{cell_style}">{cell_value}</td>'

        rows.append(f'<tr style="{row_style}">{cells}</tr>')

    if headers:
        style = """
            font-weight: bold;
            font-size: 1.25rem;
            text-align: center;
            padding: 0.5rem 1rem;
            background-color: rgba(12, 100, 192, 0.125);
        """
        skip_first_header = [""] + columns[1:]
        head_cells = "\n".join(f'<th scope="col" style="{style}">{col}</th>' for col in skip_first_header)
        head = f"<tr>{head_cells}</tr>"
    else:
        head = ""

    if totals:
        style = """
            font-weight: bold;
            font-size: 1.25rem;
            text-align: center;
            padding: 0.5rem 1rem;
            background-color: rgba(12, 100, 192, 0.125);
        """
        sums = df[columns].sum(numeric_only=True)
        total_cells = "\n".join(f'<td style="{style}">{int(s)}</td>' for s in sums)
        footer = f'<tr><th scope="row" style="{style}"></th>{total_cells}</tr>'
    else:
        footer = ""

    body = "\n".join(rows)
    ret = f"""
    <table style="{tbl_style}">
        <thead>
            {head}
        </thead>
        <tbody>
            {body}
        </tbody>
        <tfoot>
            {footer}
        </tfoot>
    </table>
    """
    return ret


def _wrap_body(body: str) -> str:
    return f"""
    <body style="font-size: 100%; margin: 0px;">
        <div style="width: 600px; padding: 1rem 1.5rem;">

            {body}

            <br><hr>

            {_footer()}

        </div>
    </body>
    """


def _footer() -> str:
    return """
    <div style="font-size: 0.75rem">
        <p>
        This report was written by
        <a href="https://github.com/richard-berg/cantori-attendance">a bot</a>...
        don't shoot the messenger!
        </p>
        <p>Data sources:</p>
        <ul>
            <li><a href="https://cantori.choirgenius.com/accounts/manage">https://cantori.choirgenius.com/accounts/manage</a></li>
            <li><a href="https://cantori.choirgenius.com/report/attendance_grid_report">https://cantori.choirgenius.com/report/attendance_grid_report</a></li>
            <li><a href="https://cantori.choirgenius.com/report/attendance_grid_forecast_report">https://cantori.choirgenius.com/report/attendance_grid_forecast_report</a></li>
            <li><a href="https://cantorinewyork.monday.com/boards/4609409564/views/112722504">https://cantorinewyork.monday.com/boards/4609409564/views/112722504</a></li>
            <li><a href="https://cantorinewyork.monday.com/boards/3767283316/views/111015403">https://cantorinewyork.monday.com/boards/3767283316/views/111015403</a></li>
        </ul>
    </div>
    """
