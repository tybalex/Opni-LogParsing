# Standard Library

# Third Party
import pandas as pd
import regex as re

# Local
from masker import LogMasker

masker = LogMasker()


def mytokenize(df_log: pd.DataFrame) -> pd.DataFrame:
    tokenized_log = []
    for _, log in df_log["parsed_log"].iteritems():
        tokens = log.split()

        tokens = re.sub(r"\\", "", str(tokens))
        tokens = re.sub(r"\'", "", str(tokens))
        tokens = tokens.translate({ord(c): "" for c in r"!@#$%^&*{}?\|`~"})

        # "http?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
        # folder "(\/|)([a-zA-Z0-9-]+\.){2,}([a-zA-Z0-9-]+)?(:[a-zA-Z0-9-]+|)(:|)"

        re_list = [
            r"([\da-fA-F]{2}:){5}[\da-fA-F]{2}",  # mac address
            r"\d{4}-\d{2}-\d{2}",  # date
            r"\d{4}\/\d{2}\/\d{2}",  # date
            "[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]{3})?",  # time
            "[0-9]{2}:[0-9]{2}:[0-9]{2}",  # time
            "[0-9]{2}:[0-9]{2}",  # time
            "0[xX][0-9a-fA-F]+",  # hexa
            r"([\(]?[0-9a-fA-F]*:){8,}[\)]?",  # httpaddress
            "^(?:[0-9]{4}-[0-9]{2}-[0-9]{2})(?:[ ][0-9]{2}:[0-9]{2}:[0-9]{2})?(?:[.,][0-9]{3})?",
            r"(\/|)([a-zA-Z0-9-]+\.){2,}([a-zA-Z0-9-]+)?(:[a-zA-Z0-9-]+|)(:|)",  # folder
        ]
        pat = r"\b(?:{})\b".format("|".join(str(v) for v in re_list))
        tokens = re.sub(pat, "<*>", str(tokens))
        # tokens = re.sub(pat, "<*>", " ".join(tokens))
        tokens = (
            tokens.replace("=", " = ")
            .replace(")", " ) ")
            .replace("(", " ( ")
            .replace("]", " ] ")
            .replace("[", " [ ")
            .lower()
        )
        tokenized_log.append(str(tokens).lstrip().replace(",", " "))

    df_log["tokenized_log"] = tokenized_log
    return df_log
