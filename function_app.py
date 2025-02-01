import logging
from asyncio import gather
from datetime import date, datetime, timedelta
from typing import Tuple
from zoneinfo import ZoneInfo

import azure.functions as func
import pandas
import yarl
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient

from choirgenius import ChoirGenius, EventType
from graph_email import send_email
from monday import get_monday_auditions, get_monday_roster
from report import (
    generate_attendance_report,
    generate_consistency_report,
    generate_projected_attendance_report,
)

AZURE_VAULT_URL = "https://cantorivault.vault.azure.net/"
MONDAY_SECRET_NAME = "monday-api-key"
CHOIRGENIUS_SECRET_USER = "choirgenius-user"
CHOIRGENIUS_SECRET_PASSWORD = "choirgenius-password"

CANTORI_CHOIRGENIUS_COM = yarl.URL("https://cantori.choirgenius.com/")

ATTENDANCE_EMAILS = [
    "attendance@cantorinewyork.com",
    "richard.berg@cantorinewyork.com",
    "sectionleaders@cantorinewyork.com",
]
CONSISTENCY_EMAILS = [
    "attendance@cantorinewyork.com",
    "richard.berg@cantorinewyork.com",
]
ERROR_EMAILS = ["richard.berg@cantorinewyork.com"]

app = func.FunctionApp()


class CantoriError(RuntimeError):
    pass


@app.route(route="timezone")
async def debug_time_zone(req: func.HttpRequest) -> func.HttpResponse:
    msg = f"""
    Current UTC time: {datetime.now(ZoneInfo("UTC"))}
    Current NYC time: {datetime.now(ZoneInfo("America/New_York"))}
    Current local time: {datetime.now()}
    """
    return func.HttpResponse(msg)


# 10PM daily, Eastern Time, from September to May
# Linux consumption apps don't support TZ, so 2AM Friday UTC is the best
# approximation we've got.  Sometimes it'll be 10pm in NYC, sometimes 9pm.
@app.schedule(schedule="0 0 2 * 9-12,1-5 *", arg_name="myTimer")
async def trigger_attendance_report(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("The timer was past due!")
        return

    await send_attendance_report()


@app.route(route="attendance_report", methods=[func.HttpMethod.POST])
async def post_attendance_report(req: func.HttpRequest):
    await send_attendance_report(force=True)


async def send_attendance_report(force: bool = False):
    try:
        roster = await get_roster()

        current_nyc_time = datetime.now(ZoneInfo("America/New_York"))
        current_nyc_date = current_nyc_time.date()
        cycle_from, cycle_to = _determine_concert_cycle(roster, current_nyc_date)

        # exclude today's rehearsal stats, until 7PM New York time
        if current_nyc_time.hour <= 19:
            rehearsals_to = current_nyc_date - timedelta(days=1)
        else:
            rehearsals_to = current_nyc_date

        async with await _get_choirgenius() as cg:
            actual_attendance, projected_attendance = await gather(
                cg.get_rehearsal_attendance(cycle_from, rehearsals_to),
                cg.get_projected_attendance(cycle_from, cycle_to, EventType.REHEARSAL),
            )

        subject, body, worth_sending = generate_attendance_report(
            actual_attendance, projected_attendance, roster, current_nyc_date, cycle_to
        )
        if worth_sending or force:
            await send_email(subject, body, ATTENDANCE_EMAILS)
    except CantoriError as e:
        await send_email("Error generating report", str(e), ERROR_EMAILS)


# 2PM Eastern daily, from September to May
@app.schedule(schedule="0 0 18 * 9-12,1-5 *", arg_name="myTimer")
async def trigger_projected_attendance_report(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("The timer was past due!")
        return

    await send_projected_attendance_report()


@app.route(route="projected_attendance_report", methods=[func.HttpMethod.POST])
async def post_projected_attendance_report(req: func.HttpRequest):
    await send_projected_attendance_report(force=True)


async def send_projected_attendance_report(force: bool = False):
    try:
        roster = await get_roster()

        current_nyc_time = datetime.now(ZoneInfo("America/New_York"))
        current_nyc_date = current_nyc_time.date()
        cycle_from, cycle_to = _determine_concert_cycle(roster, current_nyc_date)

        async with await _get_choirgenius() as cg:
            projected_attendance = await cg.get_projected_attendance(
                cycle_from, cycle_to, EventType.REHEARSAL
            )

        subject, body, worth_sending = generate_projected_attendance_report(
            projected_attendance, roster, current_nyc_date, cycle_to
        )
        if worth_sending or force:
            await send_email(subject, body, ATTENDANCE_EMAILS)
    except CantoriError as e:
        await send_email("Error generating report", str(e), ERROR_EMAILS)


# 10PM every night
@app.schedule(schedule="0 0 2 * * *", arg_name="myTimer")
async def trigger_consistency_report(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("The timer was past due!")
        return

    await send_consistency_report()


@app.route(route="consistency_report", methods=[func.HttpMethod.POST])
async def post_consistency_report(req: func.HttpRequest):
    await send_consistency_report(force=True)


async def send_consistency_report(force: bool = False):
    try:
        current_nyc_time = datetime.now(ZoneInfo("America/New_York"))
        current_nyc_date = current_nyc_time.date()
        season = _determine_season(date.today())

        async with await _get_choirgenius() as cg:
            roster, candidates, cg_active = await gather(
                get_roster(), get_audition_candidates(), cg.get_active()
            )
            cycle_from, cycle_to = _determine_concert_cycle(roster, current_nyc_date)
            projected_concert_attendance = await cg.get_projected_attendance(
                cycle_from, cycle_to, EventType.CONCERT
            )

        subject, body, worth_sending = generate_consistency_report(
            roster, candidates, cg_active, projected_concert_attendance, season, cycle_to
        )
        subject += f" (as of {current_nyc_time.strftime('%Y-%m-%d %H:%M:%S %Z')})"

        if worth_sending or force:
            await send_email(subject, body, CONSISTENCY_EMAILS)
    except CantoriError as e:
        await send_email("Error generating report", str(e), ERROR_EMAILS)


def _determine_season(today: date) -> str:
    if today.month >= 6:
        return f"{today.year}-{str(today.year + 1)[-2:]}"
    else:
        return f"{today.year - 1}-{str(today.year)[-2:]}"


def _determine_concert_cycle(roster: pandas.DataFrame, today: date) -> Tuple[date, date]:
    concert_dates = sorted(d for d in roster.columns if isinstance(d, date))
    if not concert_dates:
        raise CantoriError("Monday.com roster has no concert cycles defined")
    cycle_from = date(concert_dates[0].year, 9, 1)
    for cycle_to in concert_dates:
        if cycle_from <= today <= cycle_to:
            return cycle_from, cycle_to
        else:
            cycle_from = cycle_to + timedelta(days=1)

    raise CantoriError("Today isn't part of the season (as defined by Monday.com roster)")


async def get_roster() -> pandas.DataFrame:
    monday_api_key = await _get_monday_key()
    roster = await get_monday_roster(monday_api_key)
    logging.info(f"Got {len(roster)} roster singers from Monday.com")
    return roster


async def get_audition_candidates() -> pandas.DataFrame:
    monday_api_key = await _get_monday_key()
    candidates = await get_monday_auditions(monday_api_key)
    logging.info(f"Got {len(candidates)} audition candidates from Monday.com")
    return candidates


async def _get_monday_key() -> str:
    async with DefaultAzureCredential() as credential:
        async with SecretClient(vault_url=AZURE_VAULT_URL, credential=credential) as secret_client:
            assert isinstance(secret_client, SecretClient)  # base class not annotated with Self
            monday_api_key = await secret_client.get_secret(MONDAY_SECRET_NAME)
            if not monday_api_key.value:
                raise RuntimeError("Monday API key not found in Vault")

            return monday_api_key.value


async def _get_choirgenius() -> ChoirGenius:
    async with DefaultAzureCredential() as credential:
        async with SecretClient(vault_url=AZURE_VAULT_URL, credential=credential) as secret_client:
            assert isinstance(secret_client, SecretClient)
            user = await secret_client.get_secret(CHOIRGENIUS_SECRET_USER)
            password = await secret_client.get_secret(CHOIRGENIUS_SECRET_PASSWORD)
            if not (user.value and password.value):
                raise RuntimeError("ChoirGenius credentials not found in Vault")

            return ChoirGenius(CANTORI_CHOIRGENIUS_COM, user.value, password.value)
