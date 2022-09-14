# Standard Library
import json
from typing import List

# Third Party
import pandas as pd

LOG_FIELDS = ["message", "log", "Message", "MESSAGE", "LOG", "Log"]


def parse_json(df: pd.DataFrame) -> pd.DataFrame:
    loaded_json = []
    actual_logs = []
    for _, line in df.iterrows():
        loaded = None
        parsed_log = line["log"]
        try:
            loaded = json.loads(line["log"])
            for f in LOG_FIELDS:
                if f in loaded:
                    parsed_log = loaded[f]
                    break
        except Exception as e:
            pass
        loaded_json.append(loaded)
        actual_logs.append(parsed_log)
    df["parsed_json"] = loaded_json
    df["parsed_log"] = actual_logs
    return df


def json_parsing_postprocess(
    payload_data_df: pd.DataFrame, matched_templates: List[str]
) -> List[str]:
    for idx, line in payload_data_df.iterrows():
        if line["parsed_json"] is not None:
            json_obj = {}
            for key in line["parsed_json"]:
                if key in LOG_FIELDS:
                    json_obj[key] = matched_templates[idx]
                else:
                    if any(c.isdigit() for c in str(line["parsed_json"][key])):
                        json_obj[key] = "<*>"
                    else:
                        json_obj[key] = line["parsed_json"][key]
            matched_templates[idx] = json.dumps(json_obj)
    return matched_templates
