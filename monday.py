import json
from collections import defaultdict
from datetime import datetime

import httpx
import pandas

API_URL = "https://api.monday.com/v2"
API_VERSION = "2023-10"
DEFAULT_BOARD_ID = "4609409564"


def parse_board_item(board_item: dict[str, object], voice_part_metadata: dict[str, dict[str, str]]) -> dict:
    ret = {"Name": board_item["name"]}
    for column_value in board_item["column_values"]:  # type: ignore
        field = column_value["column"]["title"]
        try:
            second_date = field[field.index("-") : field.index(",")]
            parse_me = field.replace(second_date, "")
            cycle_end = datetime.strptime(parse_me, r"%b %d, %Y").date()
            ret[cycle_end] = column_value["text"]  # type: ignore
        except ValueError:
            ret[field] = column_value["text"]

    voice_part = ret["Voice Part"]
    ret.update(voice_part_metadata[voice_part])  # type: ignore
    return ret


def parse_roster_query(json_response) -> pandas.DataFrame:
    voice_part_metadata = parse_voice_part_column(json_response)
    items = json_response["data"]["boards"][0]["items_page"]["items"]

    roster = []
    for item in items:
        roster.append(parse_board_item(item, voice_part_metadata))
    return pandas.DataFrame.from_records(roster)


def parse_voice_part_column(json_response) -> dict[str, dict[str, str]]:
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
    headers = {"Authorization": api_key, "API-Version": API_VERSION}
    vars = {"boardId": DEFAULT_BOARD_ID}
    outer_json = {"query": query, "variables": vars}
    async with httpx.AsyncClient() as client:
        response = await client.post(url=API_URL, json=outer_json, headers=headers)
    j = json.loads(response.text)
    return parse_roster_query(j)
