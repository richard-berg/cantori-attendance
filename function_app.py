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

from choirgenius import ChoirGenius
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

ATTENDANCE_EMAIL = "attendance@cantorinewyork.com"
ERROR_EMAIL = "richard.berg@cantorinewyork.com"

app = func.FunctionApp()


class CantoriError(RuntimeError):
    pass


# 10PM every Thursday, from September to May
# time zone is determined by app setting WEBSITE_TIME_ZONE
@app.schedule(schedule="0 0 22 * 9-12,1-5 Thursday", arg_name="myTimer")
async def thursday_night_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

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
                cg.get_projected_attendance(cycle_from, cycle_to),
            )

        subject, body = generate_attendance_report(actual_attendance, projected_attendance, roster, cycle_to)
        await send_email(subject, body, ATTENDANCE_EMAIL)
    except CantoriError as e:
        await send_email("Error generating report", str(e), ERROR_EMAIL)


# 2PM every Thursday, from September to May
@app.schedule(schedule="0 0 14 * 9-12,1-5 Thursday", arg_name="myTimer")
async def thursday_afternoon_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    try:
        roster = await get_roster()

        current_nyc_time = datetime.now(ZoneInfo("America/New_York"))
        current_nyc_date = current_nyc_time.date()
        cycle_from, cycle_to = _determine_concert_cycle(roster, current_nyc_date)

        async with await _get_choirgenius() as cg:
            projected_attendance = await cg.get_projected_attendance(cycle_from, cycle_to)

        subject, body = generate_projected_attendance_report(
            projected_attendance, roster, current_nyc_date, cycle_to
        )
        await send_email(subject, body, ATTENDANCE_EMAIL)
    except CantoriError as e:
        await send_email("Error generating report", str(e), ERROR_EMAIL)


# 10PM every night
@app.schedule(schedule="0 0 22 * * *", arg_name="myTimer")
async def nightly_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    async def get_active():
        async with await _get_choirgenius() as cg:
            return await cg.get_active()

    try:
        roster, candidates, cg_active = await gather(get_roster(), get_audition_candidates(), get_active())

        current_nyc_time = datetime.now(ZoneInfo("America/New_York"))
        current_nyc_date = current_nyc_time.date()
        season = _determine_season(date.today())
        _cycle_from, cycle_to = _determine_concert_cycle(roster, current_nyc_date)

        subject, body = generate_consistency_report(roster, candidates, cg_active, season, cycle_to)
        await send_email(subject, body, ATTENDANCE_EMAIL)
    except CantoriError as e:
        await send_email("Error generating report", str(e), ERROR_EMAIL)


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
        secret_client = SecretClient(vault_url=AZURE_VAULT_URL, credential=credential)
        monday_api_key = await secret_client.get_secret(MONDAY_SECRET_NAME)
        if not monday_api_key.value:
            raise RuntimeError("Monday API key not found in Vault")

        return monday_api_key.value


async def _get_choirgenius() -> ChoirGenius:
    async with DefaultAzureCredential() as credential:
        secret_client = SecretClient(vault_url=AZURE_VAULT_URL, credential=credential)
        user = await secret_client.get_secret(CHOIRGENIUS_SECRET_USER)
        password = await secret_client.get_secret(CHOIRGENIUS_SECRET_PASSWORD)
        if not (user.value and password.value):
            raise RuntimeError("ChoirGenius credentials not found in Vault")

        return ChoirGenius(CANTORI_CHOIRGENIUS_COM, user.value, password.value)
