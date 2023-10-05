from datetime import date
from typing import Set, Tuple

import pandas


def generate_report(
    attendance: pandas.DataFrame,
    roster: pandas.DataFrame,
    cycle_to: date,
) -> Tuple[str, str]:
    """Returns: subject, body"""

    def format_singers_table(singers: Set[str]) -> str:
        singers_enriched = enrich(singers, roster)
        return name_voice_table(singers_enriched)

    def format_subtotals_table(singers: Set[str]) -> str:
        singers_enriched = enrich(singers, roster)
        counts = singers_enriched.groupby(["Voice Part", "sort_key", "color", "border"]).count()
        return name_voice_table(counts.reset_index().sort_values("sort_key"), email=False)

    def format_singers_oneline(singers: Set[str]) -> str:
        if not singers:
            return "None"
        else:
            singers_enriched = enrich(singers, roster)
            htmls = singers_enriched.apply(lambda row: format(row, "Name"), axis=1)
            return ", ".join(htmls.to_list())

    def format_singers_indented(singers: Set[str]) -> str:
        line = format_singers_oneline(singers)
        return f'<p style="margin-left: 20px">{line}</p>'

    rehearsals = [c for c in attendance.columns if isinstance(c, date)]
    concerts = [c for c in roster.columns if isinstance(c, date)]

    total_attended = attendance[rehearsals].sum(axis=1)
    attended_at_least_one = set(attendance[total_attended > 0].Name)
    attendance["Absences"] = len(rehearsals) - total_attended

    yes = ["Yes", "Partial"]
    maybe = ["Maybe"]

    singing_this_cycle = set(roster[roster[cycle_to].isin(yes)].Name)
    maybe_this_cycle = set(roster[roster[cycle_to].isin(maybe)].Name)

    other_cycles = roster[concerts].drop(columns=cycle_to)
    other_cycles_yes = set(roster[other_cycles.isin(yes).any(axis=1)].Name) - singing_this_cycle
    other_cycles_maybe = (
        set(roster[other_cycles.isin(maybe).any(axis=1)].Name) - singing_this_cycle - other_cycles_yes
    )
    gone = set(roster.Name) - singing_this_cycle - other_cycles_yes - other_cycles_maybe

    active_emails = set(roster[roster["Chorus Emails"] == "Yes"].Name)

    relevant_absences = attendance[attendance.Name.isin(singing_this_cycle) & (attendance.Absences >= 1)]
    absences_by_count = relevant_absences.groupby("Absences").agg({"Name": set}).sort_index(ascending=False)
    absence_total_table = absences_by_count.Name.apply(format_singers_oneline)
    absence_total_html = pandas.DataFrame(absence_total_table).to_html(
        header=False, escape=False, index_names=False, border=0
    )

    first_rehearsal = min(rehearsals)
    tonight = max(rehearsals)
    attended_tonight = set(attendance[attendance[tonight] == 1].Name)
    absent_tonight = singing_this_cycle - attended_tonight

    body = f"""
    <h1>Latest Rehearsal ({tonight})</h1>

    <h2><b>{len(attended_tonight)}</b> present:</h2>
    {format_subtotals_table(attended_tonight)}

    <h2><b>{len(absent_tonight)}</b> absent:</h2>
    {format_singers_table(absent_tonight)}

    <p><b><i>Please confirm whether they told us in advance, and follow up with anyone who is AWOL.</i></b></p>

    <h1>This Cycle ({first_rehearsal} to {cycle_to})</h1>

    <h2><b>{len(singing_this_cycle)}</b> singers on the concert roster:</h2>
    {format_subtotals_table(singing_this_cycle)}

    <h3>Total absences:</h3>
    {absence_total_html}
    <p>Singers with 3 or more absences are subject to make-up sessions, or being asked to sit out.</p>

    <h2>Plus, <b>{len(maybe_this_cycle)}</b> others still listed as "maybe":</h2>
    {format_singers_indented(maybe_this_cycle)}
    <p>"Maybes" are not counted as part of the roster.  <b><i>Please confirm their intentions, and
    move them to "Yes" or "No" ASAP.</i></b></p>

    <h1>Looking Ahead</h1>

    <h2><b>{len(other_cycles_yes | other_cycles_maybe)}</b> singers are sitting out:</h2>
    <ul>
        <li>
            <p><b>{len(other_cycles_yes)}</b> said they'd be back this season:</p>
            {format_singers_indented(other_cycles_yes)}
        </li>
        <li>
            <p><b>{len(other_cycles_maybe)}</b> said "maybe":</p>
            {format_singers_indented(other_cycles_maybe)}
        </li>
    </ul>

    <h2>The rest ({len(gone)}) are not expected back this season:</h2>
    {format_singers_indented(gone)}

    <h1>Consistency Checks</h1>

    <p><b><i>Please double-check that these make sense</i></b> (i.e. aren't the result of inconsistent data entry
    in ChoirGenius vs Monday.com)</p>

    <ul>

    <li>
    <p>Currently getting chorus emails, yet aren't on the concert roster for this cycle:</p>
    {format_singers_indented(active_emails - singing_this_cycle)}
    </li>

    <li>
    <p>On the current concert roster, yet aren't getting emails:</p>
    {format_singers_indented(singing_this_cycle - active_emails)}
    </li>

    <li>
    <p>Have attended rehearsal(s) this cycle, yet aren't on the current concert roster:</p>
    {format_singers_indented(attended_at_least_one - singing_this_cycle)}
    </li>

    </ul>

    <br/>
    <br/>
    <br/>
    <br/>

    <div style="font-size: 12px">
        <p>
        This report was written by
        <a href="https://github.com/richard-berg/cantori-attendance">a bot</a>...
        don't shoot the messenger!
        </p>
        <p>Data sources:</p>
        <ul>
            <li><a href="https://cantori.choirgenius.com/report/attendance_grid_report">https://cantori.choirgenius.com/report/attendance_grid_report</a></li>
            <li><a href="https://cantorinewyork.monday.com/boards/4609409564/views/112722504">https://cantorinewyork.monday.com/boards/4609409564/views/112722504</a></li>
        </ul>
    """

    subject = f"Attendance Report for {tonight}"
    return subject, body


def enrich(singers: Set[str], roster: pandas.DataFrame) -> pandas.DataFrame:
    return roster[roster.Name.isin(singers)].sort_values(["sort_key", "Name"])


def format(row: pandas.Series, column_name: str, email: bool = True) -> str:
    text_style = "color: white; text-decoration: none;"
    bg_color = f'background-color: {row["color"]};'
    border = f'border-color: {row["border"]};'
    text = row[column_name]
    span = f'<span style="{text_style} {bg_color} {border}">{text}</span>'
    if email:
        return f'<a href="mailto:{row["Email"]}">{span}</a>'
    else:
        return span


def name_voice_table(df: pandas.DataFrame, email: bool = True) -> str:
    """Two-column HTML table of {Name, Voice Part w/ color coding}

    Input have columns {Name, Voice Part, color, border}
    """

    def link_name_to_email(row: pandas.Series):
        return f'<a href="mailto:{row["Email"]}">{row["Name"]}</a>'

    name_html = df.apply(link_name_to_email, axis=1) if email else df.Name
    voice_part_html = df.apply(lambda row: format(row, "Voice Part", email=False), axis=1)
    table = df.assign(NameHtml=name_html, VoiceHtml=voice_part_html)[["NameHtml", "VoiceHtml"]]
    return table.to_html(header=False, index=False, escape=False, border=0)
