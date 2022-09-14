# Standard Library
import logging
import warnings
from collections import Counter, defaultdict
from string import punctuation
from typing import DefaultDict, List, Tuple

# Third Party
import pandas as pd
import regex as re
from pandas.core.groupby import DataFrameGroupBy

# Local
from json_parsing import parse_json
from ulp_tokenize import mytokenize
from utils import read_input, write_to_file

warnings.filterwarnings("ignore")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel("DEBUG")


def create_dataframe(matrix, tokens):
    return pd.DataFrame(data=matrix, columns=tokens)


def trans(string):
    string = " ".join(dict.fromkeys(string.split()))
    return filter(None, (word.strip(punctuation) for word in string.split()))


def getDynamicVars2(petit_group):
    petit_group["tokenized_log"] = petit_group["tokenized_log"].map(
        lambda x: " ".join(dict.fromkeys(x.split()))
    )
    petit_group["tokenized_log"] = petit_group["tokenized_log"].map(
        lambda x: " ".join(
            filter(None, (word.strip(punctuation) for word in x.split()))
        )
    )

    lst = petit_group["tokenized_log"].values.tolist()
    vec = []
    big_lst = " ".join(v for v in lst)
    this_count = Counter(big_lst.split())
    if this_count:
        max_val = max(this_count, key=this_count.get)
        for word in this_count:
            if this_count[word] < this_count[max_val]:
                vec.append(word)

    return vec


def remove_word_with_special(sentence: str) -> str:
    sentence = sentence.translate({ord(c): "" for c in r"!@#$%^&*()[]{};:,/<>?\|`~-=+"})
    length = len(splitted_sentence := sentence.split())
    finale = []
    for word in splitted_sentence:
        if len(word) > 1 and all(c.isalpha() for c in word):
            finale.append(word)

    finale.append(str(length))
    finale = " ".join(finale)
    return finale


def get_only_letters(sentence: str) -> str:
    return "".join([i for i in sentence.split() if not i.isdigit()])


def create_template(
    groups: DataFrameGroupBy,
) -> Tuple[DefaultDict[str, List[str]], DefaultDict[str, str]]:
    grouping_rule_to_template_dict = defaultdict(str)
    template_to_grouping_rule_dict = defaultdict(list)
    re_list2 = ["[ ]{1,}[-]*[0-9]+[ ]{1,}", r' "\d+" ']
    generic_re = re.compile("|".join(re_list2))

    for grouping_rule in groups.groups:
        slc = groups.get_group(grouping_rule)
        template = slc["tokenized_log"].iloc[0]  # get line 0

        if slc.size > 1:
            l = getDynamicVars2(slc)
            dynamic_token_pat = r"\b(?:{})\b".format("|".join(str(v) for v in l))
            if len(l) > 0:
                template = re.sub(dynamic_token_pat, "<*>", template)

        template = re.sub(generic_re, " <*> ", template)
        grouping_rule_to_template_dict[grouping_rule] = template
        template_to_grouping_rule_dict[template].append(grouping_rule)

    return template_to_grouping_rule_dict, grouping_rule_to_template_dict


def string_similarity(str1, str2):
    """
    let's say 2 strings with atmost x(x = 1) token diffirent is 'similar', which means they are likely belongs to the same template
    """
    diff_limit = 1
    tokens1, tokens2 = str1.split(" "), str2.split(" ")
    merged_tokens = []
    if len(tokens1) != len(tokens2):
        return False, merged_tokens
    diff_count = 0
    for idx, t in enumerate(tokens1):
        if t != tokens2[idx]:
            diff_count += 1
            merged_tokens.append("<*>")
        else:
            merged_tokens.append(t)
        if diff_count > diff_limit:
            break
    return diff_count <= diff_limit, merged_tokens


def template_merge(template_to_grouping_rule_dict):
    template_list = list(template_to_grouping_rule_dict.keys())

    first_template = template_list[0]
    ite = 1
    while ite < len(template_list):
        is_similar, merged_tokens = string_similarity(
            first_template, this_template := template_list[ite]
        )
        if is_similar:
            # then we should merge these 2, which is already done. BUT NEEDS IMPROVEMENT
            merged_template = " ".join(merged_tokens)
            for t in [first_template, this_template]:
                if merged_template != t:
                    template_to_grouping_rule_dict[merged_template].extend(
                        template_to_grouping_rule_dict[t]
                    )
                    template_to_grouping_rule_dict.pop(t)
            first_template = merged_template
        else:
            first_template = this_template
        ite += 1

    return template_to_grouping_rule_dict


def log_parsing(dataset_name: str, logformat: str, logpai_data: bool = True):
    """
    main entry for local experiment and testing
    """
    if logpai_data:
        input_file = "logs/" + dataset_name + "/" + dataset_name + "_2k.log"
    else:  ## opni data
        input_file = "opni-input/" + dataset_name
    logdf = read_input(input_file, logformat, logpai_data)
    logdf["log"] = logdf["Content"]
    logdf = parse_json(logdf)
    (
        template_to_grouping_rule_dict,
        grouping_rule_to_template_dict,
    ) = log_offline_parsing(logdf)
    log_online_matching(logdf, grouping_rule_to_template_dict)

    output_file = write_to_file(dataset_name, template_to_grouping_rule_dict)
    return output_file


def log_offline_parsing(
    logdf: pd.DataFrame,
) -> Tuple[DefaultDict[str, List[str]], DefaultDict[str, str]]:
    """
    offline training parsing, should be used as to kickoff parsing in online manner
    """
    logdf = mytokenize(logdf)

    logdf["grouping_rule"] = logdf["tokenized_log"].map(
        lambda x: remove_word_with_special(str(x))
    )
    groups = logdf.groupby("grouping_rule")
    # keys: example: INFOnodecontrollerranchermachineCreatingNewSSHKey7

    template_to_grouping_rule_dict, grouping_rule_to_template_dict = create_template(
        groups
    )

    # TODO template similarity
    # merged_template = template_merge(template_to_grouping_rule_dict)
    return template_to_grouping_rule_dict, grouping_rule_to_template_dict


def log_online_matching(
    logdf: pd.DataFrame, grouping_rule_to_template_dict: DefaultDict[str, str]
) -> List[str]:
    """
    online prediction
    """
    logdf = mytokenize(logdf)
    logdf["grouping_rule"] = logdf["tokenized_log"].map(
        lambda x: remove_word_with_special(str(x))
    )

    # TODO if no template matched, learn the new template
    matched_templates = []
    for _, row in logdf.iterrows():
        gr = row["grouping_rule"]
        template = grouping_rule_to_template_dict[gr]
        matched_templates.append(template)
    return matched_templates
