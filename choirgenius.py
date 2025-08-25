from collections import defaultdict
from datetime import date, datetime, timedelta
from enum import Enum
from io import StringIO
from types import TracebackType
from typing import Optional, Type

import httpx
import json
import pandas
import yarl
from bs4 import BeautifulSoup
from typing_extensions import Self


class EventType(Enum):
    REHEARSAL = "49"
    CONCERT = "50"


DATE_FORMAT = r"%m-%d-%Y"


class ChoirGenius:
    def __init__(self, base_url: yarl.URL, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.client = httpx.AsyncClient()

    async def login(self) -> None:
        if self.client.cookies:
            return

        login_url = self.base_url / "user/login"
        data = {
            "name": self.username,
            "pass": self.password,
            "form_id": "user_login",
        }
        response = await self.client.post(str(login_url), data=data)
        self.client.cookies = response.cookies

    async def __aenter__(self) -> Self:
        await self.login()
        return self

    async def __aexit__(
        self,
        _exc_type: Optional[Type[BaseException]] = None,
        _exc_value: Optional[BaseException] = None,
        _traceback: Optional[TracebackType] = None,
    ) -> None:
        await self.client.__aexit__()

    async def get_active(self) -> pandas.DataFrame:
        ajax_url = str(self.base_url / "g4datatables_ajax_data_router")
        data = {
            "draw": "4",
            "columns[0][data]": "formatted_name",
            "columns[0][name]": "name",
            "start": "0",
            "length": "500",
            "search[value]": 'status="active" AND role="member" AND label="Active"',
            "search[regex]": "false",
            "class": "AccountAccessTable",
        }
        response = await self.client.post(ajax_url, data=data)
        js = json.loads(response.text)
        df = pandas.DataFrame.from_records(js["data"], columns=["whole_name", "primary_email", "voice_part"])
        return df

    async def get_rehearsal_attendance(self, date_from: date, date_to: date) -> pandas.DataFrame:
        response = await self._fetch_csv_report(
            "attendance_grid_report", date_from, date_to, EventType.REHEARSAL
        )
        df = self._parse_csv_export(response.text)
        return df

    async def get_projected_attendance(
        self, date_from: date, date_to: date, event_type: EventType
    ) -> pandas.DataFrame:
        response = await self._fetch_csv_report(
            "attendance_grid_forecast_report", date_from, date_to, event_type
        )
        df = self._parse_csv_export(response.text)
        return df

    async def _fetch_csv_report(
        self, report: str, date_from: date, date_to: date, event_type: EventType
    ) -> httpx.Response:
        report_url = str(self.base_url / "report" / report)
        html_report = await self.client.get(report_url)

        soup = BeautifulSoup(html_report.text, features="html.parser")
        css_id = f'g4event-{report.replace("_", "-")}-filter'
        hidden_inputs = soup.select(f'#{css_id} input[type="hidden"]')
        hidden_fields = {str(i.attrs["name"]): str(i.attrs["value"]) for i in hidden_inputs}

        # CG seems to have switched to half-open date ranges
        date_to_exclusive = date_to + timedelta(days=1)

        data = {
            "sets[]": "g4account::role::member",
            "event_type[]": event_type.value,
            "range_start[date]": date_from.strftime(DATE_FORMAT),
            "range_end[date]": date_to_exclusive.strftime(DATE_FORMAT),
            "export": "Export",
        }
        data.update(hidden_fields)
        return await self.client.post(report_url, data=data)

    def _parse_csv_export(self, csv: str):
        # first few rows from Drupal are crap -- formatted to look pretty in Excel, not
        # for machine readability
        valid_csv = "Name" + csv.split("\r", 2)[2]
        dtype = defaultdict(lambda: "Int32", Name="str")
        df = pandas.read_csv(StringIO(valid_csv), sep=",", lineterminator="\r", dtype=dtype)
        df.columns = df.columns.map(
            lambda col: datetime.strptime(col, DATE_FORMAT).date() if col[0].isdigit() else col
        )
        return df
