from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import List, Set, Tuple

import numpy as np
import pandas


class MondayConcertState(Enum):
    YES = "Yes"
    PARTIAL = "Partial"
    MAYBE = "Maybe"
    NO = "No"


@dataclass(frozen=True)
class Email:
    subject: str
    body: str
    to: Tuple[str, ...]
    cc: Tuple[str, ...] = tuple()


SINGING_STATES = [MondayConcertState.YES.value, MondayConcertState.PARTIAL.value]
PARTIAL_STATES = [MondayConcertState.PARTIAL.value]
MAYBE_STATES = [MondayConcertState.MAYBE.value]
NO_STATES = [MondayConcertState.NO.value]


ATTENDANCE_EMAILS = (
    "attendance@cantorinewyork.com",
    "richard.berg@cantorinewyork.com",
    "sectionleaders@cantorinewyork.com",
)


def _action_item(msg: str) -> str:
    style = """
        font-size: 1.15rem;
        font-style: italic;
        background-color: #FDFD96;
    """
    return f'<span style="{style}">{msg}</span>'


def _projected_absence_details(
    df: pandas.DataFrame,
    rehearsal_col: str | date,
) -> str:
    return f"""
    <h2>Singers who are marked absent:</h2>
    {format_singers_indented(df[df[rehearsal_col] == 0])}

    <h2>Singers who haven't marked their plans in ChoirGenius:</h2>
    {format_singers_indented(df[df[rehearsal_col].isna()])}
    """


def format_mismatch_table(singers: pandas.DataFrame, monday_col: str, choirgenius_col: str) -> str:
    mismatch = (singers.cg == "both") & (singers[monday_col] != singers[choirgenius_col])

    df = singers[mismatch]
    if df.empty:
        return "None"

    df = df.rename(columns={monday_col: "Monday", choirgenius_col: "ChoirGenius"})
    return _table(df, columns=["Name", "Monday", "ChoirGenius"], headers=True)


def format_singers_oneline(singers: pandas.DataFrame) -> str:
    if singers.empty:
        return "None"

    def link_email_and_highlight_section(row: pandas.Series):
        style = f"""
            display: inline-block;
            text-decoration: none;
            color: white;
            background-color: {row["color"]};
            border: 1px;
            padding: 0.5rem 1rem;
        """
        return f'<a href="mailto:{row["Email"]}" style="{style}">{row["Name"]}</a>'

    htmls = singers.apply(link_email_and_highlight_section, axis=1)
    return "\n".join(htmls.to_list())


def format_singers_indented(singers: pandas.DataFrame) -> str:
    line = format_singers_oneline(singers)
    return f'<p style="margin-left: 1.5rem">{line}</p>'


def format_absence_totals(singers: pandas.DataFrame) -> str:
    df = singers.groupby(["Absences_total", "Absences_actual", "Absences_projected"]).agg({"Name": set})
    df = df.sort_index(ascending=False).reset_index()

    def format_name_aggregation(row: pandas.Series) -> str:
        aggregated_names: Set[str] = row["Name"]
        join = pandas.DataFrame(aggregated_names, columns=["Name"]).merge(singers, on="Name")
        join = join.sort_values(["sort_key", "Name"])
        return format_singers_oneline(join)

    df = df.assign(Names_Formatted=df.apply(format_name_aggregation, axis=1))
    col_names = {
        "Absences_total": "Total",
        "Absences_actual": "Actual",
        "Absences_projected": "Projected",
        "Names_Formatted": "Singers (click to email)",
    }
    df = df.rename(columns=col_names)
    return _table(df, columns=list(col_names.values()), headers=True)


def format_subtotals_table(df: pandas.DataFrame, indicators: dict[str, pandas.Series]) -> str:
    """Indicators: a map from column name -> boolean Series."""
    df = df.assign(**indicators)

    indicator_col_names = list(indicators.keys())
    groupby = ["Voice Part", "sort_key", "color"]
    all_cols = groupby + indicator_col_names
    display_cols = [c for c in all_cols if c not in ["sort_key", "color"]]

    df = df[all_cols].groupby(groupby).sum().reset_index().sort_values("sort_key")
    return _table(df, columns=display_cols, headers=True, totals=True)


def _fill_and_sort(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Fill: df has been joined against ChoirGenius, which may not be 1:1, so we
    want to fill in NAs where we can.

    Sort: always by SATB, then by Name.
    """
    replacements = {
        "color": df.color.fillna("black"),
        "sort_key": df.sort_key.fillna(float("inf")),
        "Email": df.Email.fillna("email-is-missing"),
    }

    if "whole_name" in df.columns:
        # We've been joined against a ChoirGenius member query (not just an
        # attendance query), so we have more data with which to fill in gaps
        replacements.update(
            {
                "Name": df.Name.fillna(df.whole_name),
                "Email": df.Email.fillna(df.EMAIL),
                "Voice Part": df["Voice Part"].fillna(df.voice_part),
            }
        )

    df = df.assign(**replacements)
    df = df.sort_values(["sort_key", "Name"])
    return df


def _table(df: pandas.DataFrame, columns: List[str], headers: bool = False, totals: bool = False) -> str:
    tbl_style = """
        border-collapse: collapse;
        border: 2px solid #eee;
    """

    col_map = {col: n for n, col in enumerate(df.columns)}

    rows: List[str] = []
    for i, row in enumerate(df.itertuples(index=False)):
        row_bg = "white" if i % 2 == 0 else "#eee"
        row_style = f"""
            background-color: {row_bg};
        """
        cells = ""
        for j, col in enumerate(columns):
            weight = "bold" if j == 0 else "normal"

            if col == "Name" and "Email" in col_map:
                cell_value = f"""
                <a href="mailto:{row[col_map["Email"]]}" style="text-decoration:none;">
                    {row[col_map["Name"]]}
                </a>
                """
            else:
                cell_value = row[col_map[col]]

            if col == "Voice Part" and "color" in col_map:
                cell_style = f"""
                    padding: 0.5rem 2rem;
                    color: white;
                    background-color: {row[col_map["color"]]};
                    text-align: center;
                    font-weight: {weight};
                """
            else:
                align = "center" if isinstance(cell_value, (int, np.integer)) else "left"
                cell_style = f"""
                    padding: 0.5rem 1rem;
                    color: black;
                    text-align: {align};
                    font-weight: {weight};
                """

            cells += f'\n<td style="{cell_style}">{cell_value}</td>'

        rows.append(f'<tr style="{row_style}">{cells}</tr>')

    if headers:
        style = """
            font-weight: bold;
            font-size: 1.25rem;
            text-align: center;
            padding: 0.5rem 1rem;
            background-color: rgba(12, 100, 192, 0.125);
        """
        skip_first_header = [""] + columns[1:]
        head_cells = "\n".join(f'<th scope="col" style="{style}">{col}</th>' for col in skip_first_header)
        head = f"<tr>{head_cells}</tr>"
    else:
        head = ""

    if totals:
        style = """
            font-weight: bold;
            font-size: 1.25rem;
            text-align: center;
            padding: 0.5rem 1rem;
            background-color: rgba(12, 100, 192, 0.125);
        """
        sums = df[columns].sum(numeric_only=True)
        total_cells = "\n".join(f'<td style="{style}">{int(s)}</td>' for s in sums)
        footer = f'<tr><th scope="row" style="{style}"></th>{total_cells}</tr>'
    else:
        footer = ""

    body = "\n".join(rows)
    ret = f"""
    <table style="{tbl_style}">
        <thead>
            {head}
        </thead>
        <tbody>
            {body}
        </tbody>
        <tfoot>
            {footer}
        </tfoot>
    </table>
    """
    return ret


def _wrap_body(body: str) -> str:
    return f"""
    <body style="font-size: 100%; margin: 0px;">
        <div style="width: 600px; padding: 1rem 1.5rem;">

            {body}

            <br><hr>

            {_footer()}

        </div>
    </body>
    """


def _footer() -> str:
    return """
    <div style="font-size: 0.75rem">
        <p>
        This report was written by
        <a href="https://github.com/richard-berg/cantori-attendance">a bot</a>...
        don't shoot the messenger!
        </p>
        <p>Data sources:</p>
        <ul>
            <li><a href="https://cantori.choirgenius.com/accounts/manage">https://cantori.choirgenius.com/accounts/manage</a></li>
            <li><a href="https://cantori.choirgenius.com/report/attendance_grid_report">https://cantori.choirgenius.com/report/attendance_grid_report</a></li>
            <li><a href="https://cantori.choirgenius.com/report/attendance_grid_forecast_report">https://cantori.choirgenius.com/report/attendance_grid_forecast_report</a></li>
            <li><a href="https://cantorinewyork.monday.com/boards/9192120251/views/195540167">https://cantorinewyork.monday.com/boards/9192120251/views/195540167</a></li>
            <li><a href="https://cantorinewyork.monday.com/boards/3767283316/views/111015403">https://cantorinewyork.monday.com/boards/3767283316/views/111015403</a></li>
        </ul>
    </div>
    """
