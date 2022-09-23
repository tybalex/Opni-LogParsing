# Standard Library
import json
import logging
import time
from typing import Any, Dict

# Third Party
import pandas as pd
import regex as re

LOG_FIELDS = ["message", "log"]

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel("DEBUG")


pat = re.compile(
    r"\{(?:[^{}]|(?R))*\}"
)  # requires regex lib bc '(?R)' is recursive and its not supported by re


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
    parsed_json_infos = []
    actual_logs = []
    severitys = []
    st = time.time()
    for _, line in df.iterrows():
        parsed_info = []
        severity = ""
        actual_log = parsed_log = str(line["log"]).rstrip().lstrip()
        if (
            matched := match_embedded_json_string(pat, parsed_log)
        ) is not None:  # can auto adjust it based on the statistics of how many full-json and embedded-json
            if matched.span()[0] == 0 and matched.span()[1] == len(
                parsed_log
            ):  ## full json?
                try:
                    parsed_json_str = _flatten_json(json.loads(parsed_log))
                except Exception as e:
                    parsed_json_str = None
                    parsed_info.append("wrong-format")
                if parsed_json_str is not None:
                    parsed_info.append("full-json")
                    for f in parsed_json_str.keys():
                        if f.lower() in LOG_FIELDS:
                            parsed_info.append(f)
                            parsed_log = parsed_json_str[f]
                            embedded_json = match_embedded_json_string(
                                pat, str(parsed_log)
                            )
                            if embedded_json is not None:
                                parsed_info.append(embedded_json.span())
                            break
                    for f in parsed_json_str.keys():
                        if f.lower() == "severity":
                            severity = parsed_json_str[f]
                    actual_log = json.dumps(parsed_json_str)
            else:
                try:
                    parsed_json_str = _flatten_json(
                        json.loads(parsed_log[matched.span()[0] : matched.span()[1]])
                    )
                except Exception as e:
                    parsed_json_str = None
                    parsed_info.append("wrong-format")
                if parsed_json_str is not None:
                    parsed_info.append("partial-json")
                    parsed_info.append(matched.span())
                    actual_log = (
                        actual_log[: matched.span()[0]]
                        + json.dumps(parsed_json_str)
                        + actual_log[matched.span()[1] :]
                    )

        severitys.append(severity)
        parsed_json_infos.append(parsed_info)
        actual_logs.append(actual_log)
    logger.info(f"json parsing time taken : {time.time() - st} on {len(df)} data point")
    df["parsed_info"] = parsed_json_infos
    df["parsed_log"] = actual_logs
    df["severity"] = severitys
    return df


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
    x1 = json.dumps(d1)
    # print(x1)
    x2 = 'PaymentService#Charge invoked with request {"amount":{"currency_code":"USD","units":"0","nanos":0},"credit_card":{"credit_card_number":"4432-8015-6152-0454","credit_card_cvv":672,"credit_card_expiration_year":2039,"credit_card_expiration_month":1}}'
    x3 = '2022-09-20T23:21:10.205Z	INFO	loggingexporter/logging_exporter.go:56	MetricsExporter	"#metrics": 34'
    res = match_embedded_json_string(pat, str(x2))
    print(res.group())
    l1 = [{"log": x1}] * 10000
    l2 = [{"log": x2}] * 10000
    l3 = [{"log": x3}] * 10000
    df1 = pd.DataFrame(l1)
    df2 = pd.DataFrame(l2)
    df3 = pd.DataFrame(l3)
    t1 = time.time()
    parse_json(df1)
    parse_json(df2)
    parse_json(df3)
    print(time.time() - t1)
