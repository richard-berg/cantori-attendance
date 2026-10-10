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
        absence_totals = soup.find("h2", string="At risk (3+ projected absences):").find_next("table")

        self.assertEqual(actual.loc[0, "Attended"], 1)
        self.assertEqual(actual.loc[0, "Absences"], 2)
        self.assertEqual(actual.loc[0, "Partials"], 1)
        self.assertEqual(
            [th.get_text(strip=True) for th in this_week.find_next("table").select("thead th")],
            ["", "Present", "Absent", "Late"],
        )
        self.assertIn("Soprano 1 0 1", weekly_rows)
        self.assertIn("Alto 1 0 0", weekly_rows)
        self.assertIn("Late", absence_totals.get_text(" ", strip=True))
        self.assertEqual(
            [th.get_text(" ", strip=True) for th in absence_totals.select("thead th")],
            ["", "Projected Total", "So Far", "Late"],
        )
        self.assertEqual(
            absence_totals.select_one("tbody tr").get_text(" ", strip=True),
            "Partial Singer 3 2 1",
        )
        this_cycle_heading = next(h for h in soup.find_all("h2") if "said they'll participate" in h.get_text())
        self.assertEqual(
            [th.get_text(" ", strip=True) for th in this_cycle_heading.find_next("table").select("thead th")],
            ["", "October Roster", "At Risk"],
        )
        this_cycle_rows = [
            row.get_text(" ", strip=True) for row in this_cycle_heading.find_next("table").select("tbody tr")
        ]
        self.assertEqual(this_cycle_rows, ["Soprano 1 1", "Alto 1 0"])
        self.assertNotIn("Full Singer", absence_totals.get_text(" ", strip=True))
        self.assertNotIn(
            "Partial Singer", soup.find("h2", string="Absence details:").find_next("table").get_text()
        )
        partial_details = soup.find("h2", string="Singers who were late:")
        self.assertIsNotNone(partial_details)
        self.assertEqual(partial_details.find_next("p").get_text(" ", strip=True), "Partial Singer")
        self.assertNotIn("Full Singer", partial_details.find_next("p").get_text())
        self.assertTrue(partial_details.find_next("h1").get_text(" ", strip=True).startswith("This Cycle"))
        self.assertNotIn("Next Rehearsal", email.body)
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

    def test_at_risk_threshold_and_section_sort(self):
        cycle_to = date(2026, 10, 31)
        past = [date(2026, 9, 3), date(2026, 9, 10), date(2026, 9, 17), date(2026, 9, 24)]
        future = [date(2026, 10, 1), date(2026, 10, 8)]
        names = ["Zed Alto", "Amy Soprano", "Bob Soprano", "Future Risk", "Future Only"]
        roster = pandas.DataFrame(
            {
                "Name": names,
                cycle_to: ["Yes"] * 5,
                "Voice Part": ["Alto", "Soprano", "Soprano", "Alto", "Alto"],
                "sort_key": [2, 1, 1, 2, 2],
                "color": ["blue", "red", "red", "blue", "blue"],
                "Email": [f"{n}@example.com" for n in range(5)],
                "Chorus Emails": ["Yes"] * 5,
            }
        )
        actual = pandas.DataFrame(
            {
                "Name": names,
                past[0]: [0, 0, 0, 0, 1],
                past[1]: [0, 0, 0, 1, 1],
                past[2]: [0, 1, 0, 1, 1],
                past[3]: [0, 1, 1, 1, 1],
            }
        )
        projected = actual.copy()
        projected[future[0]] = [1, 1, 1, 0, 0]
        projected[future[1]] = [1, 1, 1, 0, 0]

        email, _ = generate_attendance_report(actual, projected, roster, past[-1], past[0], cycle_to)
        soup = BeautifulSoup(email.body, "html.parser")
        rows = [
            row.get_text(" ", strip=True)
            for row in soup.find("h2", string="At risk (3+ projected absences):")
            .find_next("table")
            .select("tbody tr")
        ]
        this_cycle_heading = next(h for h in soup.find_all("h2") if "said they'll participate" in h.get_text())
        this_cycle_rows = [
            row.get_text(" ", strip=True) for row in this_cycle_heading.find_next("table").select("tbody tr")
        ]

        # projected_total >= 3 only; "Amy Soprano" (2 so far) and "Future Only" (2 projected) are omitted
        self.assertEqual(rows, ["Bob Soprano 3 3 0", "Future Risk 3 1 0", "Zed Alto 4 4 0"])
        at_risk_rows = soup.find("h2", string="At risk (3+ projected absences):").find_next("table").select("tbody tr")
        projected_cells = [row.find_all("td")[1] for row in at_risk_rows]
        self.assertEqual(projected_cells[0].b.get_text(), "3")
        self.assertNotIn("#FDFD96", str(projected_cells[0]))
        self.assertIn("#FDFD96", str(projected_cells[2].b))
        self.assertIsNone(at_risk_rows[0].find_all("td")[2].b)
        self.assertEqual(this_cycle_rows, ["Soprano 2 1", "Alto 3 2"])
        self.assertIsNone(next((h for h in soup.find_all("h2") if "Thresholds" in h.get_text()), None))

        absence_details = soup.find("h2", string="Absence details:").find_next("table")
        self.assertEqual(
            [row.get_text(" ", strip=True) for row in absence_details.select("tbody tr")],
            ["Zed Alto Marked 4th"],
        )
        self.assertIn("background-color: blue", str(absence_details.find("td")))
        self.assertIn("#FDFD96", str(absence_details.find("b", string="4th")))
        checklist = [li.get_text(" ", strip=True) for li in absence_details.find_next("ul").find_all("li")]
        self.assertEqual(len(checklist), 3)
        self.assertTrue(checklist[0].startswith("Stephen/Attendance : confirm"))
        self.assertEqual(checklist[1], "Janara : then fix tonight's attendance in ChoirGenius.")
        self.assertTrue(checklist[2].startswith("4th absence : Mark checks their preparedness"))

    def test_absence_details_numbers_absences_and_explains_thresholds(self):
        cycle_to = date(2026, 10, 31)
        past = [date(2026, 9, 3), date(2026, 9, 12)]
        names = ["Jumper", "Steady", "Second"]
        roster = pandas.DataFrame(
            {
                "Name": names,
                cycle_to: ["Yes"] * 3,
                "Voice Part": ["Soprano"] * 3,
                "sort_key": [1] * 3,
                "color": ["red"] * 3,
                "Email": [f"{n}@example.com" for n in range(3)],
                "Chorus Emails": ["Yes"] * 3,
            }
        )
        actual = pandas.DataFrame({"Name": names, past[0]: [0, 0, 1], past[1]: [0, 1, 0]})
        actual.attrs[EVENT_WEIGHTS] = {past[1]: RETREAT_WEIGHT}
        projected = actual.copy()
        projected.loc[2, past[1]] = float("nan")

        email, _ = generate_attendance_report(
            actual.copy(), projected.copy(), roster, past[-1], past[0], cycle_to
        )
        soup = BeautifulSoup(email.body, "html.parser")
        absence_details = soup.find("h2", string="Absence details:").find_next("table")
        checklist = absence_details.find_next("ul").get_text(" ", strip=True)

        # Jumper goes 1 -> 3 at the retreat; Second goes 0 -> 2 without telling us; Steady attended
        self.assertEqual(
            [row.get_text(" ", strip=True) for row in absence_details.select("tbody tr")],
            ["Jumper Marked 3rd", "Second AWOL? 2nd"],
        )
        self.assertIsNotNone(absence_details.find("b", string="3rd"))
        self.assertIsNone(absence_details.find("b", string="2nd"))
        self.assertIn("Follow up with AWOL singers.", checklist)
        self.assertIn("2nd absence : Section Leaders check in verbally", checklist)
        self.assertIn("3rd absence : Section Leaders email", checklist)
        self.assertNotIn("Mark", checklist)

        quiet_email, _ = generate_attendance_report(
            actual[["Name", past[0]]].copy(), projected[["Name", past[0]]].copy(), roster, past[0], past[0], cycle_to
        )
        quiet_soup = BeautifulSoup(quiet_email.body, "html.parser")
        quiet_details = quiet_soup.find("h2", string="Absence details:").find_next("table")
        self.assertEqual(
            [row.get_text(" ", strip=True) for row in quiet_details.select("tbody tr")],
            ["Jumper Marked", "Steady Marked"],
        )
        self.assertNotIn("check in verbally", quiet_email.body)
        self.assertNotIn("Follow up with AWOL", quiet_email.body)

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
