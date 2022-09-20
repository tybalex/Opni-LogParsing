# Standard Library
import json
import logging
import time
from typing import Any, Dict, List

# Third Party
import pandas as pd
import regex as re

LOG_FIELDS = ["message", "log"]

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel("DEBUG")


pat = re.compile(r"\{(?:[^{}]|(?R))*\}")  # requires regex lib


def match_embedded_json_string(compiled_pattern, string: str):
    res = compiled_pattern.search(string)
    return res


def _flatten_nested_json(
    data: Any,
    key_string: str,
    flattened_dict: Dict[str, Any],
    separator: str,
) -> Dict[str, Any]:
    """
    data : Any
        Type dependent on types contained within nested Json
    key_string : str
        New key (with separator(s) in) for data
    flattened_dict : dict
        The new flattened Json dict
    separator : str, default '.'
        Nested records will generate names separated by sep,
        e.g., for sep='.', { 'foo' : { 'bar' : 0 } } -> foo.bar
    """
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{key_string}{separator}{key}"
            _flatten_nested_json(
                data=value,
                key_string=new_key
                if new_key[: len(separator)] != separator
                else new_key[len(separator) :],
                flattened_dict=flattened_dict,
                separator=separator,
            )
    else:
        flattened_dict[key_string] = data
    return flattened_dict


def _flatten_json(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """
    data : dict or list of dicts
    separator : str, default '.'
        Nested records will generate names separated by sep,
        e.g., for sep='.', { 'foo' : { 'bar' : 0 } } -> foo.bar
    Returns
    -------
    dict or list of dicts
    """
    top_dict_ = {k: v for k, v in data.items() if not isinstance(v, dict)}
    nested_dict_ = _flatten_nested_json(
        data={k: v for k, v in data.items() if isinstance(v, dict)},
        key_string="",
        flattened_dict={},
        separator=separator,
    )
    return {**top_dict_, **nested_dict_}


def parse_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    main function to parse json string, detected and parse nested/embedded json
    """
    loaded_json = []
    actual_logs = []
    st = time.time()
    for _, line in df.iterrows():
        loaded = None
        parsed_log = line["log"]
        # if match_embedded_json_string(pat, str(line["log"])) is not None:
        try:
            loaded = _flatten_json(json.loads(line["log"]))
        except Exception as e:
            pass
        if loaded is not None:
            for f in loaded.keys():
                if f.lower() in LOG_FIELDS:
                    parsed_log = loaded[f]
                    embedded_json = match_embedded_json_string(pat, str(parsed_log))
                    if embedded_json is not None:
                        loaded["log_embedded_json"] = embedded_json.group()
                    break

        loaded_json.append(loaded)
        actual_logs.append(parsed_log)
    logger.info(f"json parsing time taken : {time.time() - st} on {len(df)} data point")
    df["embedded_json_tested"] = [
        l["log_embedded_json"] if l is not None and "log_embedded_json" in l else ""
        for l in loaded_json
    ]
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
                    json_obj[key] = "*"
                    # if any(c.isdigit() for c in str(line["parsed_json"][key])):
                    #     json_obj[key] = "<*>"
                    # else:
                    #     json_obj[key] = line["parsed_json"][key]
            matched_templates[idx] = json.dumps(json_obj)
    return matched_templates


if __name__ == "__main__":

    d1 = {
        "severity": "info",
        "time": 1663210318369,
        "instant": {"epochSecond": 1663210318, "nanoOfSecond": 472990000},
        "pid": 1,
        "hostname": "paymentservice-5ccdf79b99-ztjpf",
        "name": "paymentservice-server",
        "message": 'PaymentService#Charge invoked with request {"amount":{"currency_code":"USD","units":"11238","nanos":999999995},"credit_card":{"credit_card_number":"4432-8015-6152-0454","credit_card_cvv":672,"credit_card_expiration_year":2039,"credit_card_expiration_month":1}}',
        "v": 1,
    }
    x1 = _flatten_json(d1, ".")
    # print(x1)
    x2 = 'PaymentService#Charge invoked with request {"amount":{"currency_code":"USD","units":"0","nanos":0},"credit_card":{"credit_card_number":"4432-8015-6152-0454","credit_card_cvv":672,"credit_card_expiration_year":2039,"credit_card_expiration_month":1}}'
    res = match_embedded_json_string(pat, str(x2))
    print(res.group())
    l1 = [{"log": x2}] * 10000
    df = pd.DataFrame(l1)
    t1 = time.time()
    parse_json(df)
    print(time.time() - t1)
