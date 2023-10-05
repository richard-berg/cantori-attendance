from collections import defaultdict
from datetime import date, datetime
from io import StringIO
from types import TracebackType
from typing import Optional, Type

import httpx
import pandas
import yarl
from bs4 import BeautifulSoup
from typing_extensions import Self


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

    async def get_attendance(self, date_from: date, date_to: date) -> pandas.DataFrame:
        report_url = str(self.base_url / "report/attendance_grid_report")
        html_report = await self.client.get(report_url)

        soup = BeautifulSoup(html_report)
        hidden_inputs = soup.select('#g4event-attendance-grid-report-filter input[type="hidden"]')
        hidden_fields = {i.attrs["name"]: i.attrs["value"] for i in hidden_inputs}

        date_format = r"%m-%d-%Y"
        data = {
            "sets[]": "g4account::role::member",
            "event_type[]": "49",
            "range_start[date]": date_from.strftime(date_format),
            "range_end[date]": date_to.strftime(date_format),
            "export": "Export",
        }
        data.update(hidden_fields)
        response = await self.client.post(report_url, data=data)
        df = self._parse_csv_export(response.text)
        df.columns = df.columns.map(
            lambda col: datetime.strptime(col, r"%m-%d-%Y").date() if col[0].isdigit() else col
        )
        return df

    def _parse_csv_export(self, csv: str):
        # first few rows from Drupal are crap -- formatted to look pretty in Excel, not
        # for machine readability
        valid_csv = "Name" + csv.split("\r", 2)[2]
        dtype = defaultdict(lambda: "Int32", Name="str")
        df = pandas.read_csv(StringIO(valid_csv), sep=",", lineterminator="\r", dtype=dtype)
        return df
