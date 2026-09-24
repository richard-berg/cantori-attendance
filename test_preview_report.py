from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import function_app
import preview_report
from report_utils import Email


class PreviewReportTests(IsolatedAsyncioTestCase):
    async def test_reports_preview_even_when_not_worth_sending(self):
        email = Email("Report", "<body>Live report</body>", ("test@example.com",))
        reports = (
            ("attendance_report", "build_attendance_report"),
            ("projected_attendance_report", "build_projected_attendance_report"),
            ("consistency_report", "build_consistency_report"),
        )

        with TemporaryDirectory() as directory, patch.object(
            preview_report, "OUTPUT_DIR", Path(directory)
        ), patch.object(function_app, "send_email", new_callable=AsyncMock) as send:
            for report, builder in reports:
                with self.subTest(report=report), patch.object(
                    preview_report, builder, new_callable=AsyncMock, return_value=(email, False)
                ):
                    path = await preview_report.preview_report(report)
                    self.assertEqual(path.read_text(encoding="utf-8"), email.body)

            send.assert_not_awaited()

    async def test_member_nags_preview_only_first_email(self):
        first = Email("First", "<body>First member</body>", ("first@example.com",))

        def nags():
            yield first
            raise AssertionError("Preview consumed more than one member nag")

        with TemporaryDirectory() as directory, patch.object(
            preview_report, "OUTPUT_DIR", Path(directory)
        ), patch.object(
            preview_report, "build_member_nags", new_callable=AsyncMock, return_value=nags()
        ), patch.object(function_app, "send_email", new_callable=AsyncMock) as send:
            path = await preview_report.preview_report("member_nags")
            self.assertEqual(path.read_text(encoding="utf-8"), first.body)
            send.assert_not_awaited()

    async def test_no_member_nags_does_not_overwrite_previous_preview(self):
        with TemporaryDirectory() as directory, patch.object(
            preview_report, "OUTPUT_DIR", Path(directory)
        ), patch.object(
            preview_report, "build_member_nags", new_callable=AsyncMock, return_value=iter(())
        ):
            path = Path(directory) / "member_nags.html"
            path.write_text("previous preview", encoding="utf-8")
            self.assertIsNone(await preview_report.preview_report("member_nags"))
            self.assertEqual(path.read_text(encoding="utf-8"), "previous preview")

    async def test_scheduled_report_still_requires_worth_sending(self):
        email = Email("Report", "<body>Live report</body>", ("test@example.com",))
        with patch.object(
            function_app, "build_attendance_report", new_callable=AsyncMock, return_value=(email, False)
        ), patch.object(function_app, "send_email", new_callable=AsyncMock) as send:
            await function_app.send_attendance_report()
            send.assert_not_awaited()

            await function_app.send_attendance_report(force=True)
            send.assert_awaited_once_with(email)