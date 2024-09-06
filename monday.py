import json
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import httpx
import pandas

API_URL = "https://api.monday.com/v2"
API_VERSION = "2023-10"
ROSTER_BOARD_ID = "6792979476"
AUDITION_BOARD_ID = "3767283316"


def _parse_roster_item(
    board_item: dict[str, Any], voice_part_metadata: dict[str, dict[str, str]]
) -> dict[str | date, str]:
    ret: dict[str | date, str] = {"Name": board_item["name"]}
    for column_value in board_item["column_values"]:
        field = column_value["column"]["title"]
        try:
            if "-" in field:
                second_date = field[field.index("-") : field.index(",")]
                parse_me = field.replace(second_date, "")
            else:
                parse_me = field

            # might raise
            cycle_end = datetime.strptime(parse_me, r"%b %d, %Y").date()
            ret[cycle_end] = column_value["text"]
        except ValueError:
            ret[field] = column_value["text"]

    voice_part = ret["Voice Part"]
    ret.update(voice_part_metadata[voice_part])  # type: ignore
    return ret


def _parse_roster_query(json_response) -> pandas.DataFrame:
    voice_part_metadata = _parse_voice_part_column(json_response)
    items = json_response["data"]["boards"][0]["items_page"]["items"]

    roster = []
    for item in items:
        roster.append(_parse_roster_item(item, voice_part_metadata))
    return pandas.DataFrame.from_records(roster)


def _parse_voice_part_column(json_response) -> dict[str, dict[str, str]]:
    """Returns map from voice_part -> {sort_key: X, color: Y, border: Z}"""
    columns = json_response["data"]["boards"][0]["columns"]
    voice_col = next(c for c in columns if c["title"] == "Voice Part")
    voice_meta = json.loads(voice_col["settings_str"])
    ret: dict[str, dict[str, str]] = defaultdict(dict)
    for id, sort_key in voice_meta["labels_positions_v2"].items():
        if id in voice_meta["labels"]:
            ret[voice_meta["labels"][id]]["sort_key"] = sort_key
    for id, color_info in voice_meta["labels_colors"].items():
        if id in voice_meta["labels"]:
            ret[voice_meta["labels"][id]]["color"] = color_info["color"]
            ret[voice_meta["labels"][id]]["border"] = color_info["border"]
    return ret


def parse_audition_query(json_response) -> pandas.DataFrame:
    items = json_response["data"]["boards"][0]["items_page"]["items"]

    records = []
    for item in items:
        records.append(_parse_audition_item(item))
    return pandas.DataFrame.from_records(records)


def _parse_audition_item(candidate: dict[str, Any]) -> dict:
    ret = {"Name": candidate["name"], "Group": candidate["group"]["title"]}
    for column_value in candidate["column_values"]:
        field = column_value["column"]["title"]
        ret[field] = column_value["text"]

    return ret


async def get_monday_roster(api_key: str) -> pandas.DataFrame:
    query = """
    query RosterDump($boardId: ID!) {
        boards(ids: [$boardId]) {
            columns {
              title
              settings_str
            }
            items_page(limit:500) {
                items {
                    name
                    column_values {
                        column {
                            title
                        }
                        text
                    }
                }
            }
        }
    }
    """
    results = await _query_monday(api_key, ROSTER_BOARD_ID, query)
    return _parse_roster_query(results)


async def get_monday_auditions(api_key: str) -> pandas.DataFrame:
    query = """
    query Auditions($boardId: ID!) {
        boards(ids: [$boardId]) {
            items_page(limit:500) {
                items {
                    name
                    group {
                       title
                    }
                    column_values(ids: ["email", "audition_result"]) {
                        column {
                            title
                        }
                        text
                    }
                }
            }
        }
    }
    """
    results = await _query_monday(api_key, AUDITION_BOARD_ID, query)
    return parse_audition_query(results)


async def _query_monday(api_key: str, board_id: str, graphql_query: str) -> Any:
    headers = {"Authorization": api_key, "API-Version": API_VERSION}
    vars = {"boardId": board_id}
    outer_json = {"query": graphql_query, "variables": vars}
    async with httpx.AsyncClient() as client:
        response = await client.post(url=API_URL, json=outer_json, headers=headers)
    return json.loads(response.text)
