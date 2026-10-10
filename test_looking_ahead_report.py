from datetime import date
from unittest import TestCase

import pandas
from bs4 import BeautifulSoup, Tag

from looking_ahead_report import (
    LOOKING_AHEAD_EMAILS,
    WELCOME_CC,
    LookingAheadData,
    generate_looking_ahead_report,
    generate_welcome_emails,
    looking_ahead_send_dates,
    next_concert_cycle,
)

CYCLE_TO = date(2026, 10, 31)
NEXT_CYCLE_TO = date(2026, 12, 20)
LATER_CYCLE_TO = date(2027, 3, 14)
FIRST_REHEARSAL = date(2026, 9, 3)
MIDPOINT = date(2026, 10, 2)
FINAL = date(2026, 10, 17)
NEXT_FIRST_REHEARSAL = date(2026, 11, 5)

SINGERS = {
    # name: (current cycle, next cycle, later cycle)
    "Steady Singer": ("Yes", "Yes", "Yes"),
    "Maybe Next": ("Yes", "Maybe", "Yes"),
    "Returning Member": ("No", "Partial", "Yes"),
    "Joining Auditionee": ("", "Yes", "Yes"),
    "Waiting Auditionee": ("", "Maybe", ""),
    "Departing Singer": ("Partial", "No", "No"),
    "Gone Singer": ("No", "No", "No"),
    "Later Singer": ("No", "No", "Yes"),
}


def _data(today: date = MIDPOINT, next_first_rehearsal: date | None = NEXT_FIRST_REHEARSAL):
    names = list(SINGERS)
    roster = pandas.DataFrame(
        {
            "Name": names,
            CYCLE_TO: [s[0] for s in SINGERS.values()],
            NEXT_CYCLE_TO: [s[1] for s in SINGERS.values()],
            LATER_CYCLE_TO: [s[2] for s in SINGERS.values()],
            "Voice Part": ["Soprano"] * len(names),
            "sort_key": range(len(names)),
            "color": ["red"] * len(names),
            "Email": [f"{n.split()[0].lower()}@example.com" for n in names],
        }
    )
    candidates = pandas.DataFrame(
        {
            "Name": ["Joining Auditionee", "Waiting Auditionee", "Steady Singer"],
            "Group": ["2026-27", "2026-27", "2025-26"],
            "RESULT": ["Accepted", "Accepted", "Accepted"],
        }
    )
    cg_active = pandas.DataFrame({"whole_name": [n for n in names if n != "Returning Member"]})
    return LookingAheadData(
        roster=roster,
        candidates=candidates,
        cg_active=cg_active,
        season="2026-27",
        today=today,
        cycle_first_rehearsal=FIRST_REHEARSAL,
        cycle_to=CYCLE_TO,
        next_cycle_to=NEXT_CYCLE_TO,
        next_first_rehearsal=next_first_rehearsal,
    )


def _names_after(soup: BeautifulSoup, text: str) -> list[str]:
    heading = soup.find(lambda tag: tag.name in ("h2", "p") and text in tag.get_text())
    assert heading is not None, f"Section not found: {text}"
    singers = heading.find_next("p", style=lambda s: s is not None and "margin-left" in s)
    assert isinstance(singers, Tag)
    return [a.get_text(strip=True) for a in singers.find_all("a")]


class LookingAheadScheduleTests(TestCase):
    def test_send_dates(self):
        self.assertEqual(looking_ahead_send_dates(FIRST_REHEARSAL, CYCLE_TO), (MIDPOINT, FINAL))

    def test_next_concert_cycle(self):
        roster = _data().roster
        self.assertEqual(next_concert_cycle(roster, CYCLE_TO), NEXT_CYCLE_TO)
        self.assertEqual(next_concert_cycle(roster, NEXT_CYCLE_TO), LATER_CYCLE_TO)
        self.assertIsNone(next_concert_cycle(roster, LATER_CYCLE_TO))

    def test_report_only_worth_sending_on_send_dates(self):
        for today, expected in (
            (MIDPOINT, True),
            (FINAL, True),
            (date(2026, 10, 3), False),
            (date(2026, 10, 16), False),
        ):
            with self.subTest(today=today):
                _, worth_sending = generate_looking_ahead_report(_data(today))
                self.assertEqual(worth_sending, expected)

    def test_welcome_emails_only_worth_sending_on_final_date(self):
        for today, expected in ((MIDPOINT, False), (FINAL, True), (date(2026, 10, 18), False)):
            with self.subTest(today=today):
                _, worth_sending = generate_welcome_emails(_data(today))
                self.assertEqual(worth_sending, expected)


class LookingAheadReportTests(TestCase):
    def test_sections(self):
        email, _ = generate_looking_ahead_report(_data())
        soup = BeautifulSoup(email.body, "html.parser")

        self.assertEqual(email.to, LOOKING_AHEAD_EMAILS)
        self.assertIn("December", email.subject)
        self.assertIn(str(NEXT_FIRST_REHEARSAL), email.body)
        self.assertEqual(
            _names_after(soup, 'listed as "Maybe" for December'), ["Maybe Next", "Waiting Auditionee"]
        )
        self.assertEqual(_names_after(soup, "members are returning"), ["Returning Member"])
        self.assertEqual(_names_after(soup, "will join us for December"), ["Joining Auditionee"])
        self.assertEqual(_names_after(soup, "haven't committed to December"), ["Waiting Auditionee"])
        self.assertEqual(_names_after(soup, "Not on the ChoirGenius 'Active' list"), ["Returning Member"])
        self.assertEqual(_names_after(soup, "current singers aren't listed"), ["Departing Singer"])
        self.assertEqual(_names_after(soup, "not expected back this season"), ["Gone Singer"])
        self.assertIn(f"will be emailed a welcome note on {FINAL}", email.body)

    def test_welcome_note_on_final_date(self):
        email, _ = generate_looking_ahead_report(_data(FINAL))
        self.assertIn("is being emailed a welcome note today", email.body)

    def test_unknown_next_first_rehearsal(self):
        email, _ = generate_looking_ahead_report(_data(next_first_rehearsal=None))
        self.assertIn("first rehearsal on TBD", email.body)


class WelcomeEmailTests(TestCase):
    def test_returners_and_joining_auditionees_get_distinct_emails(self):
        emails, _ = generate_welcome_emails(_data(FINAL))
        by_recipient = {e.to: e for e in emails}

        self.assertEqual(set(by_recipient), {("returning@example.com",), ("joining@example.com",)})

        returning = by_recipient[("returning@example.com",)]
        self.assertEqual(returning.subject, "Welcome back to Cantori, Returning!")
        self.assertEqual(returning.cc, WELCOME_CC)
        self.assertIn("Thursday, November 5", returning.body)
        self.assertIn("December 20", returning.body)
        self.assertIn("mark your attendance plans", returning.body)

        joining = by_recipient[("joining@example.com",)]
        self.assertEqual(joining.subject, "Welcome to Cantori, Joining!")
        self.assertIn("Welcome to Cantori!", joining.body)

    def test_unknown_first_rehearsal_links_to_calendar(self):
        emails, _ = generate_welcome_emails(_data(FINAL, next_first_rehearsal=None))
        self.assertIn("rehearsal schedule will be posted", emails[0].body)

    def test_singers_without_email_are_skipped(self):
        data = _data(FINAL)
        data.roster.loc[data.roster.Name == "Returning Member", "Email"] = None
        emails, _ = generate_welcome_emails(data)
        self.assertEqual([e.to for e in emails], [("joining@example.com",)])
