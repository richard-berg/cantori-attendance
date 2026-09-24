import argparse
import asyncio
from pathlib import Path

from function_app import (
    build_attendance_report,
    build_consistency_report,
    build_member_nags,
    build_projected_attendance_report,
)


OUTPUT_DIR = Path(__file__).parent / "previews"
REPORTS = (
    "attendance_report",
    "projected_attendance_report",
    "consistency_report",
    "member_nags",
)


async def preview_report(report: str) -> Path | None:
    if report == "attendance_report":
        email, _ = await build_attendance_report()
    elif report == "projected_attendance_report":
        email, _ = await build_projected_attendance_report()
    elif report == "consistency_report":
        email, _ = await build_consistency_report()
    elif report == "member_nags":
        email = next(iter(await build_member_nags()), None)
    else:
        raise ValueError(f"Unknown report: {report}")

    if email is None:
        print("No member needs a nag; any previous preview file is stale.")
        return None

    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / f"{report}.html"
    path.write_text(email.body, encoding="utf-8")
    print(f"Preview written to {path}")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preview a report using live ChoirGenius and Monday data")
    parser.add_argument("report", choices=REPORTS)
    asyncio.run(preview_report(parser.parse_args().report))