from datetime import date, datetime
from unittest import TestCase

import pandas
from bs4 import BeautifulSoup

from attendance_report import generate_attendance_report
from choirgenius import EVENT_WEIGHTS, RETREAT_WEIGHT, ChoirGenius
from consistency_report import generate_consistency_report
from projected_report import generate_projected_attendance_report


class PartialAttendanceTests(TestCase):
    def test_csv_parses_partial_and_missing_attendance(self):
        rehearsal = date(2026, 9, 24)
        csv = "heading\rheading\r,09-24-2026\rPartial Singer,0.5\rFull Singer,1\rUnmarked Singer,\r"

        attendance = ChoirGenius._parse_csv_export(None, csv)

        self.assertEqual(attendance.loc[0, rehearsal], 0.5)
        self.assertEqual(attendance.loc[1, rehearsal], 1)
        self.assertTrue(pandas.isna(attendance.loc[2, rehearsal]))

    def test_actual_partial_counts_as_present_but_not_absent(self):
        cycle_to = date(2026, 10, 31)
        past = [date(2026, 9, 3), date(2026, 9, 17), date(2026, 9, 24)]
        future_partial = date(2026, 9, 30)
        future_absent = date(2026, 10, 7)
        roster = pandas.DataFrame(
            {
                "Name": ["Partial Singer", "Full Singer"],
                cycle_to: ["Yes", "Yes"],
                "Voice Part": ["Soprano", "Alto"],
                "sort_key": [1, 2],
                "color": ["red", "blue"],
                "Email": ["partial@example.com", "full@example.com"],
                "Chorus Emails": ["Yes", "Yes"],
            }
        )
        actual = pandas.DataFrame(
            {
                "Name": ["Partial Singer", "Full Singer"],
                past[0]: [0, 1],
                past[1]: [0, 1],
                past[2]: [0.5, 1],
            }
        )
        projected = pandas.DataFrame(
            {
                "Name": ["Partial Singer", "Full Singer"],
                past[0]: [0, 1],
                past[1]: [0, 1],
                past[2]: [0.5, 1],
                future_partial: [0.5, 1],
                future_absent: [0, 1],
            }
        )

        email, _ = generate_attendance_report(actual, projected, roster, past[-1], past[0], cycle_to)
        soup = BeautifulSoup(email.body, "html.parser")
        this_week = soup.find("h1", string=lambda text: text and text.startswith("This Week"))
        weekly_rows = [
            row.get_text(" ", strip=True) for row in this_week.find_next("table").select("tbody tr")
        ]
        absence_totals = soup.find("h2", string="Absence Totals:").find_next("table")

        self.assertEqual(actual.loc[0, "Attended"], 1)
        self.assertEqual(actual.loc[0, "Absences"], 2)
        self.assertEqual(actual.loc[0, "Partials"], 1)
        self.assertIn("Soprano 1 1 0", weekly_rows)
        self.assertIn("Alto 1 0 0", weekly_rows)
        self.assertIn("Partial", absence_totals.get_text(" ", strip=True))
        self.assertEqual(
            absence_totals.select_one("tbody tr").get_text(" ", strip=True),
            "3 2 1 1 Partial Singer",
        )
        self.assertNotIn("Full Singer", absence_totals.get_text(" ", strip=True))
        self.assertNotIn(
            "Partial Singer", soup.find("h2", string="Absence details:").find_next("table").get_text()
        )
        partial_details = soup.find("h2", string="Partial Details:")
        self.assertIsNotNone(partial_details)
        self.assertEqual(
            partial_details.find_next("table").select_one("tbody tr").get_text(" ", strip=True),
            "Partial Singer Soprano",
        )
        self.assertNotIn("Full Singer", partial_details.find_next("table").get_text())
        self.assertEqual(
            partial_details.find_next("h1").get_text(" ", strip=True), f"Next Rehearsal ({future_partial})"
        )
        marked_absent = soup.find("h2", string="Singers who are marked absent:")
        projected_partial = marked_absent.find_next("h2")
        self.assertEqual(projected_partial.get_text(strip=True), "Singers who are marked partial:")
        self.assertIn("Partial Singer", projected_partial.find_next("p").get_text())
        self.assertNotIn("Full Singer", projected_partial.find_next("p").get_text())
        self.assertEqual(
            projected_partial.find_next("h2").get_text(strip=True),
            "Singers who haven't marked their plans in ChoirGenius:",
        )
        self.assertNotIn('still listed as "maybe"', email.body)
        self.assertNotIn("please confirm their intentions", email.body)
        self.assertNotIn("Looking Ahead", email.body)

        roster_with_maybe = roster.copy()
        roster_with_maybe.loc[1, cycle_to] = "Maybe"
        maybe_email, _ = generate_attendance_report(
            actual.copy(), projected.copy(), roster_with_maybe, past[-1], past[0], cycle_to
        )
        maybe_soup = BeautifulSoup(maybe_email.body, "html.parser")
        maybe_heading = next(
            (
                heading
                for heading in maybe_soup.find_all("h2")
                if 'still listed as "maybe"' in heading.get_text()
            ),
            None,
        )
        self.assertIsNotNone(maybe_heading)
        self.assertIn("Full Singer", maybe_heading.find_next("p").get_text())
        self.assertIn("please confirm their intentions", maybe_email.body)

    def test_projected_partial_is_confirmed(self):
        rehearsal = date(2026, 9, 25)
        cycle_to = date(2026, 10, 31)
        roster = pandas.DataFrame(
            {
                "Name": ["Partial Singer"],
                cycle_to: ["Yes"],
                "Voice Part": ["Soprano"],
                "sort_key": [1],
                "color": ["red"],
                "Email": ["partial@example.com"],
            }
        )
        projected = pandas.DataFrame({"Name": ["Partial Singer"], rehearsal: [0.5]})

        email, _ = generate_projected_attendance_report(projected, roster, rehearsal, cycle_to)
        soup = BeautifulSoup(email.body, "html.parser")
        row = soup.select_one("tbody tr")

        self.assertEqual(row.get_text(" ", strip=True), "Soprano 1 0 0")
        self.assertIn(
            "Partial Singer",
            soup.find("h2", string="Singers who are marked partial:").find_next("p").get_text(),
        )

    def test_csv_parses_retreat_weights(self):
        csv = (
            "\nwhole_name,429,402\r"
            ',Thursday Rehearsal,"Fall Retreat, Day 1"\r'
            ",10-15-2026,10-17-2026\r"
            "Some Singer,1,0\r"
        )

        attendance = ChoirGenius._parse_csv_export(None, csv)

        self.assertEqual(
            attendance.attrs[EVENT_WEIGHTS], {date(2026, 10, 15): 1, date(2026, 10, 17): RETREAT_WEIGHT}
        )

    def test_retreat_counts_as_two_absences_and_partial_as_one(self):
        cycle_to = date(2026, 10, 31)
        rehearsal, retreat, future_retreat = date(2026, 10, 15), date(2026, 10, 17), date(2026, 10, 24)
        names = ["Absent Singer", "Partial Singer", "Full Singer"]
        weights = {rehearsal: 1, retreat: RETREAT_WEIGHT, future_retreat: RETREAT_WEIGHT}
        roster = pandas.DataFrame(
            {
                "Name": names,
                cycle_to: ["Yes"] * 3,
                "Voice Part": ["Soprano", "Alto", "Tenor"],
                "sort_key": [1, 2, 3],
                "color": ["red", "blue", "green"],
                "Email": ["a@example.com", "p@example.com", "f@example.com"],
                "Chorus Emails": ["Yes"] * 3,
            }
        )
        actual = pandas.DataFrame({"Name": names, rehearsal: [1, 0.5, 1], retreat: [0, 0.5, 1]})
        actual.attrs[EVENT_WEIGHTS] = weights
        projected = pandas.DataFrame(
            {"Name": names, rehearsal: [1, 1, 1], retreat: [0, 0.5, 1], future_retreat: [0.5, 0, 1]}
        )
        projected.attrs[EVENT_WEIGHTS] = weights

        generate_attendance_report(actual, projected, roster, retreat, date(2026, 9, 1), cycle_to)

        self.assertEqual(actual.Absences.to_list(), [2, 1, 0])
        self.assertEqual(actual.Partials.to_list(), [0, 2, 0])
        self.assertEqual(projected.Absences.to_list(), [1, 2, 0])

    def test_projected_partial_concert_counts_as_singing(self):
        concert = date(2026, 10, 31)
        roster = pandas.DataFrame(
            {
                "Name": ["Partial Singer"],
                concert: ["Maybe"],
                "Voice Part": ["Soprano"],
                "sort_key": [1],
                "color": ["red"],
                "Email": ["partial@example.com"],
                "EMAIL": ["partial@example.com"],
            }
        )
        candidates = pandas.DataFrame(
            {"Name": ["Partial Singer"], "Group": ["2026-27"], "RESULT": ["Accepted"]}
        )
        active = pandas.DataFrame(
            {
                "whole_name": ["Partial Singer"],
                "primary_email": ["partial@example.com"],
                "voice_part": ["Soprano"],
            }
        )
        projected = pandas.DataFrame({"Name": ["Partial Singer"], concert: [0.5]})

        email, worth_sending = generate_consistency_report(
            roster, candidates, active, projected, "2026-27", concert, datetime(2026, 9, 24)
        )

        self.assertEqual(projected.loc[0, "Concerts_Marked_Singing"], 1)
        self.assertTrue(worth_sending)
        self.assertIn("Partial Singer", email.body)
