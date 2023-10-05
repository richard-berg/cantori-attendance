import logging
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
from monday import get_monday_roster
from report import generate_report

AZURE_VAULT_URL = "https://cantorivault.vault.azure.net/"
MONDAY_SECRET_NAME = "monday-api-key"
CHOIRGENIUS_SECRET_USER = "choirgenius-user"
CHOIRGENIUS_SECRET_PASSWORD = "choirgenius-password"

CANTORI_CHOIRGENIUS_COM = yarl.URL("https://cantori.choirgenius.com/")

app = func.FunctionApp()


class CantoriError(RuntimeError):
    pass


# 10PM every Thursday, from September to May
# time zone is determined by app setting WEBSITE_TIME_ZONE
@app.schedule(schedule="0 0 22 * 9-12,1-5 Thursday", arg_name="myTimer")
async def thursday_night_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    roster = await get_roster()

    current_nyc_time = datetime.now(ZoneInfo("America/New_York"))
    current_nyc_date = current_nyc_time.date()

    try:
        cycle_from, cycle_to = _determine_concert_cycle(roster, current_nyc_date)
    except CantoriError as err:
        logging.warn(err)
        return

    # exclude today's rehearsal stats, until 7PM New York time
    if current_nyc_time.hour <= 19:
        rehearsals_to = current_nyc_date - timedelta(days=1)
    else:
        rehearsals_to = current_nyc_date
    attendance = await get_attendance(cycle_from, rehearsals_to)

    subject, body = generate_report(attendance, roster, cycle_to)
    await send_email(subject, body)


def _determine_concert_cycle(roster: pandas.DataFrame, today: date) -> Tuple[date, date]:
    concert_dates = sorted(d for d in roster.columns if isinstance(d, date))
    if not concert_dates:
        raise ValueError("Monday.com roster has no concert cycles defined")
    cycle_from = date(concert_dates[0].year, 9, 1)
    for cycle_to in concert_dates:
        if cycle_from <= today <= cycle_to:
            return cycle_from, cycle_to
        else:
            cycle_from = cycle_to + timedelta(days=1)

    raise CantoriError("Today isn't part of the season (as defined by Monday.com roster)")


async def get_attendance(date_from: date, date_to: date) -> pandas.DataFrame:
    async with DefaultAzureCredential() as credential:
        secret_client = SecretClient(vault_url=AZURE_VAULT_URL, credential=credential)
        user = await secret_client.get_secret(CHOIRGENIUS_SECRET_USER)
        password = await secret_client.get_secret(CHOIRGENIUS_SECRET_PASSWORD)
        if not (user.value and password.value):
            raise RuntimeError("ChoirGenius credentials not found in Vault")

        async with ChoirGenius(CANTORI_CHOIRGENIUS_COM, user.value, password.value) as cg:
            return await cg.get_attendance(date_from, date_to)


async def get_roster() -> pandas.DataFrame:
    async with DefaultAzureCredential() as credential:
        secret_client = SecretClient(vault_url=AZURE_VAULT_URL, credential=credential)
        monday_api_key = await secret_client.get_secret(MONDAY_SECRET_NAME)
        if not monday_api_key.value:
            raise RuntimeError("Monday API key not found in Vault")
        roster = await get_monday_roster(monday_api_key.value)
        logging.info(f"Got {len(roster)} users from Monday.com")
        return roster
