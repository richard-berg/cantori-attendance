from datetime import date
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import yarl

from choirgenius import DATE_FORMAT, EVENT_WEIGHTS, RETREAT_WEIGHT, ChoirGenius, EventType

CSV = (
    "heading\r"
    ',Thursday Rehearsal,"Fall Retreat, Day 1",Concert\r'
    ",11-12-2026,11-13-2026,11-14-2026\r"
    "Some Singer,1,0,0\r"
)


def _cg() -> ChoirGenius:
    return ChoirGenius(yarl.URL("https://example.com/"), "user", "password")


class ChoirGeniusDateRangeTests(IsolatedAsyncioTestCase):
    async def test_events_after_date_to_are_dropped(self):
        cg = _cg()
        with patch.object(cg, "_fetch_csv_report", new_callable=AsyncMock, return_value=MagicMock(text=CSV)):
            projected = await cg.get_projected_attendance(
                date(2026, 11, 1), date(2026, 11, 13), EventType.CONCERT
            )
            actual = await cg.get_rehearsal_attendance(date(2026, 11, 1), date(2026, 11, 13))

        for df in (projected, actual):
            self.assertEqual(
                [c for c in df.columns if isinstance(c, date)], [date(2026, 11, 12), date(2026, 11, 13)]
            )
            self.assertEqual(
                df.attrs[EVENT_WEIGHTS], {date(2026, 11, 12): 1, date(2026, 11, 13): RETREAT_WEIGHT}
            )
        await cg.client.aclose()

    async def test_query_includes_the_day_after_date_to(self):
        cg = _cg()
        cg.client = MagicMock()
        cg.client.get = AsyncMock(return_value=MagicMock(text="<html></html>"))
        cg.client.post = AsyncMock()

        await cg._fetch_csv_report(
            "attendance_grid_report", date(2026, 11, 1), date(2026, 11, 14), EventType.CONCERT
        )

        data = cg.client.post.await_args.kwargs["data"]
        self.assertEqual(data["range_start[date]"], date(2026, 11, 1).strftime(DATE_FORMAT))
        self.assertEqual(data["range_end[date]"], date(2026, 11, 15).strftime(DATE_FORMAT))
