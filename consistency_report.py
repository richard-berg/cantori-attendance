from datetime import date, datetime
from typing import Tuple
import pandas

from report_utils import (
    MAYBE_STATES,
    NO_STATES,
    PARTIAL_STATES,
    SINGING_STATES,
    Email,
    _fill_and_sort,
    _wrap_body,
    format_mismatch_table,
    format_singers_indented,
)


CONSISTENCY_EMAILS = (
    "attendance@cantorinewyork.com",
    "richard.berg@cantorinewyork.com",
)


def generate_consistency_report(
    roster: pandas.DataFrame,
    candidates: pandas.DataFrame,
    cg_active: pandas.DataFrame,
    projected_concert_attendance: pandas.DataFrame,
    season: str,
    cycle_to: date,
    current_nyc_time: datetime,
) -> Tuple[Email, bool]:
    """Returns: Email, worth_sending"""
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

    subject = f"ChoirGenius vs Monday.com consistency check failed! (as of {current_nyc_time.strftime('%Y-%m-%d %H:%M:%S %Z')})"
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

    email = Email(subject=subject, body=_wrap_body(body), to=CONSISTENCY_EMAILS)
    return email, worth_sending
