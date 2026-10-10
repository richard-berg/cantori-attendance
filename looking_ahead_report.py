from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional, Tuple

import pandas

from report_utils import (
    MAYBE_STATES,
    SINGING_STATES,
    Email,
    _action_item,
    _fill_and_sort,
    _wrap_body,
    format_singers_indented,
)


LOOKING_AHEAD_EMAILS = (
    "attendance@cantorinewyork.com",
    "richard.berg@cantorinewyork.com",
    "janara.kellerman@cantorinewyork.com",
)

WELCOME_CC = LOOKING_AHEAD_EMAILS

FINAL_SEND_DAYS_BEFORE_CONCERT = 14

CHOIRGENIUS_CALENDAR = "https://cantori.choirgenius.com/calendar/events"


@dataclass(frozen=True)
class LookingAheadData:
    roster: pandas.DataFrame
    candidates: pandas.DataFrame
    cg_active: pandas.DataFrame
    season: str
    today: date
    cycle_first_rehearsal: date
    cycle_to: date
    next_cycle_to: date
    next_first_rehearsal: Optional[date]


@dataclass(frozen=True)
class _Groups:
    next_maybes: pandas.Series
    returners: pandas.Series
    joining_auditionees: pandas.Series
    uncommitted_auditionees: pandas.Series
    departures: pandas.Series
    gone: pandas.Series
    not_in_cg: pandas.Series


def next_concert_cycle(roster: pandas.DataFrame, cycle_to: date) -> Optional[date]:
    later = [c for c in roster.columns if isinstance(c, date) and c > cycle_to]
    return min(later) if later else None


def looking_ahead_send_dates(cycle_first_rehearsal: date, cycle_to: date) -> Tuple[date, date]:
    """Returns: (midpoint, final) send dates for the current cycle."""
    midpoint = cycle_first_rehearsal + (cycle_to - cycle_first_rehearsal) // 2
    final = cycle_to - timedelta(days=FINAL_SEND_DAYS_BEFORE_CONCERT)
    return midpoint, final


def generate_looking_ahead_report(data: LookingAheadData) -> Tuple[Email, bool]:
    """Returns: Email, worth_sending"""
    join = _fill_and_sort(data.roster.copy())
    groups = _classify(join, data)

    midpoint, final = looking_ahead_send_dates(data.cycle_first_rehearsal, data.cycle_to)
    worth_sending = data.today in (midpoint, final)

    next_name = _cycle_name(data.next_cycle_to)
    if data.today == final:
        welcome_note = "Each of these singers is being emailed a welcome note today."
    elif data.today > final:
        welcome_note = f"Each of these singers was emailed a welcome note on {final}."
    else:
        welcome_note = f"Each of these singers will be emailed a welcome note on {final}."

    first_rehearsal = data.next_first_rehearsal or "TBD"

    body = f"""
    <h1>Looking Ahead to {next_name}</h1>
    <p>
    The current cycle ends with the concert on {data.cycle_to}.
    The {next_name} cycle begins with its first rehearsal on {first_rehearsal},
    leading up to the concert on {data.next_cycle_to}.
    </p>

    <br><hr>

    <h2><b>{groups.next_maybes.sum()}</b> singers are listed as "Maybe" for {next_name}:</h2>
    {format_singers_indented(join[groups.next_maybes])}
    <p>
    {_action_item('<b>Janara</b>: please contact each of them, and move them to "Yes" or "No" on Monday.com.')}
    </p>

    <h2><b>{groups.returners.sum()}</b> members are returning for {next_name}:</h2>
    {format_singers_indented(join[groups.returners])}
    <p>{welcome_note}</p>

    <h2><b>{(groups.joining_auditionees | groups.uncommitted_auditionees).sum()}</b> accepted auditionees are on the sidelines this cycle:</h2>
    <ul>
        <li>
            <p><b>{groups.joining_auditionees.sum()}</b> will join us for {next_name}:</p>
            {format_singers_indented(join[groups.joining_auditionees])}
            <p>{welcome_note}</p>
        </li>
        <li>
            <p><b>{groups.uncommitted_auditionees.sum()}</b> haven't committed to {next_name}:</p>
            {format_singers_indented(join[groups.uncommitted_auditionees])}
            <p>
            {_action_item('<b>Janara</b>: please reach out, and update their status on Monday.com.')}
            </p>
        </li>
    </ul>

    <h2>Not on the ChoirGenius 'Active' list, yet listed for {next_name}*:</h2>
    {format_singers_indented(join[groups.not_in_cg])}
    <p>
    {_action_item('<b>Janara</b>: please activate them in ChoirGenius, so they can mark attendance and download music.')}
    </p>
    <p style="font-size: 0.75rem">*including "Maybes"</p>

    <br><hr>

    <h1>FYI</h1>

    <h2><b>{groups.departures.sum()}</b> current singers aren't listed for {next_name}:</h2>
    {format_singers_indented(join[groups.departures])}

    <h2>The rest ({groups.gone.sum()}) are not expected back this season:</h2>
    {format_singers_indented(join[groups.gone])}
    """

    email = Email(
        subject=f"Looking ahead to {next_name} (as of {data.today})",
        body=_wrap_body(body),
        to=LOOKING_AHEAD_EMAILS,
    )
    return email, worth_sending


def generate_welcome_emails(data: LookingAheadData) -> Tuple[List[Email], bool]:
    """Returns: Emails, worth_sending"""
    join = _fill_and_sort(data.roster.copy())
    groups = _classify(join, data)

    _, final = looking_ahead_send_dates(data.cycle_first_rehearsal, data.cycle_to)
    worth_sending = data.today == final

    emails = [
        _welcome_email(row, data, returning=True)
        for _, row in join[groups.returners].iterrows()
        if _has_email(row)
    ]
    emails += [
        _welcome_email(row, data, returning=False)
        for _, row in join[groups.joining_auditionees].iterrows()
        if _has_email(row)
    ]
    return emails, worth_sending


def _classify(join: pandas.DataFrame, data: LookingAheadData) -> _Groups:
    accepted_names = data.candidates.loc[
        (data.candidates.Group == data.season) & (data.candidates.RESULT == "Accepted"), "Name"
    ]
    accepted = join.Name.isin(accepted_names)

    singing_now = join[data.cycle_to].isin(SINGING_STATES)
    maybe_now = join[data.cycle_to].isin(MAYBE_STATES)
    singing_next = join[data.next_cycle_to].isin(SINGING_STATES)
    maybe_next = join[data.next_cycle_to].isin(MAYBE_STATES)

    future_cycles = [c for c in join.columns if isinstance(c, date) and c > data.cycle_to]
    expected_later = join[future_cycles].isin(SINGING_STATES + MAYBE_STATES).any(axis=1)

    sidelined_auditionees = accepted & ~singing_now

    return _Groups(
        next_maybes=maybe_next,
        returners=~singing_now & singing_next & ~accepted,
        joining_auditionees=sidelined_auditionees & singing_next,
        uncommitted_auditionees=sidelined_auditionees & ~singing_next,
        departures=singing_now & ~(singing_next | maybe_next),
        gone=~(singing_now | maybe_now | expected_later | sidelined_auditionees),
        not_in_cg=(singing_next | maybe_next) & ~join.Name.isin(data.cg_active.whole_name),
    )


def _welcome_email(row: pandas.Series, data: LookingAheadData, returning: bool) -> Email:
    first_name = row["Name"].split()[0]
    next_name = _cycle_name(data.next_cycle_to)
    concert = _month_day(data.next_cycle_to)

    if data.next_first_rehearsal:
        first_rehearsal = f"Our first rehearsal is on <b>{data.next_first_rehearsal.strftime('%A')}, {_month_day(data.next_first_rehearsal)}</b>."
    else:
        first_rehearsal = f'The rehearsal schedule will be posted on the <a href="{CHOIRGENIUS_CALENDAR}">ChoirGenius calendar</a>.'

    if returning:
        subject = f"Welcome back to Cantori, {first_name}!"
        greeting = f"Welcome back! We're delighted that you'll be singing with us again for the {next_name} cycle (concert on {concert})."
    else:
        subject = f"Welcome to Cantori, {first_name}!"
        greeting = f"Welcome to Cantori! We're thrilled that you'll be joining us for the {next_name} cycle (concert on {concert})."

    body = f"""
    <h2>Dear {first_name},</h2>

    <p>{greeting}</p>

    <p>{first_rehearsal}</p>

    <p>
    Please <a href="{CHOIRGENIUS_CALENDAR}">mark your attendance plans</a> in ChoirGenius
    for each rehearsal in the new cycle (through {concert}).
    Having everyone's plans up-to-date is super helpful for rehearsal strategy.
    </p>

    <p>See you soon!</p>
    """
    return Email(subject=subject, body=_wrap_body(body), to=(row["Email"],), cc=WELCOME_CC)


def _has_email(row: pandas.Series) -> bool:
    return isinstance(row["Email"], str) and "@" in row["Email"]


def _cycle_name(cycle_to: date) -> str:
    return cycle_to.strftime("%B")


def _month_day(d: date) -> str:
    # Python doesn't have a day-of-month formatter w/o leading zero
    return d.strftime("%B") + f" {d.day}"
