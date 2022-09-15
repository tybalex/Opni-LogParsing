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
        tokens = log.rstrip()

        tokens = re.sub(r"\\", "", tokens)
        tokens = re.sub(r"\'", "", tokens)
        tokens = tokens.translate({ord(c): "" for c in r"!@#$%^&*{}?\|`~"})

        # "http?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
        # folder "(\/|)([a-zA-Z0-9-]+\.){2,}([a-zA-Z0-9-]+)?(:[a-zA-Z0-9-]+|)(:|)"
        re_list = [
            r"([\da-fA-F]{2}:){5}[\da-fA-F]{2}",  # mac address
            "(http|ftp|https)://([\\w_-]+(?:(?:\\.*[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?",  # URL from masker
            "\\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2]\\d|3[0-1])[T|\\s](?:[0-1]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d+|)[(?:Z|(?:\\+|\\-)(?:\\d{2}):?(?:\\d{2}))]",  # utc date from masker
            "[IWEF]\\d{4}\\s\\d{2}:\\d{2}:\\d{2}[\\.\\d+]*",  # klog date from masker
            "(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\s+(\\d{1,2}) (2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9])",  # custom date from masker
            r"\d{4}-\d{2}-\d{2}",  # regular date1
            r"\d{4}\/\d{2}\/\d{2}",  # regular date2
            "[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]{3})?",  # time
            "[0-9]{2}:[0-9]{2}:[0-9]{2}",  # time
            "[0-9]{2}:[0-9]{2}",  # time
            "[a-z0-9]+[\\._]?[a-z0-9]+[@]\\w+[.]\\w{2,3}",  # email from masker
            "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\/\\d{1,3})((?=[^A-Za-z0-9])|$)",  # IP1 from masker
            "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})((?=[^A-Za-z0-9])|$)",  # IP2 from masker
            "0[xX][0-9a-fA-F]+",  # hexa
            # r"([\(]?[0-9a-fA-F]*:){8,}[\)]?",  # httpaddress
            "^(?:[0-9]{4}-[0-9]{2}-[0-9]{2})(?:[ ][0-9]{2}:[0-9]{2}:[0-9]{2})?(?:[.,][0-9]{3})?",
            r"(\/|)([a-zA-Z0-9-]+\.){2,}([a-zA-Z0-9-]+)?(:[a-zA-Z0-9-]+|)(:|)",  # folder
        ]
        pat = r"\b(?:{})\b".format("|".join(str(v) for v in re_list))
        tokens = re.sub(pat, "<*>", tokens)
        # tokens = re.sub(pat, "<*>", " ".join(tokens))
        tokens = (
            tokens.replace("=", " = ")
            .replace(")", " ) ")
            .replace("(", " ( ")
            .replace("]", " ] ")
            .replace("[", " [ ")
            .lower()
        )
        tokenized_log.append(tokens.lstrip().replace(",", " "))

    df_log["tokenized_log"] = tokenized_log
    return df_log
