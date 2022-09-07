import re
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
import itertools
import collections
import json
import pandas as pd
import csv

import logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel("DEBUG")

class Trie():
    """Regex::Trie in Python. Creates a Trie out of a list of words. The trie can be exported to a Regex pattern.
    The corresponding Regex should match much faster than a simple Regex union."""

    def __init__(self):
        self.data = {}

    def add(self, word):
        ref = self.data
        for char in word:
            ref[char] = char in ref and ref[char] or {}
            ref = ref[char]
        ref[''] = 1

    def dump(self):
        return self.data

    def quote(self, char):
        return re.escape(char)

    def _pattern(self, pData):
        data = pData
        if "" in data and len(data.keys()) == 1:
            return None

        alt = []
        cc = []
        q = 0
        for char in sorted(data.keys()):
            if isinstance(data[char], dict):
                try:
                    recurse = self._pattern(data[char])
                    alt.append(self.quote(char) + recurse)
                except:
                    cc.append(self.quote(char))
            else:
                q = 1
        cconly = not len(alt) > 0

        if len(cc) > 0:
            if len(cc) == 1:
                alt.append(cc[0])
            else:
                alt.append('[' + ''.join(cc) + ']')

        if len(alt) == 1:
            result = alt[0]
        else:
            result = "(?:" + "|".join(alt) + ")"

        if q:
            if cconly:
                result += "?"
            else:
                result = "(?:%s)?" % result
        return result

    def pattern(self):
        return self._pattern(self.dump())
    

def trie_regex_from_words(words):
    trie = Trie()
    for word in words:
        trie.add(word)
    return re.compile(r"\b" + trie.pattern() + r"\b", re.IGNORECASE)

def get_less_frequent_words(lst):
    lst2 = [x.split() for x in lst]
    all_words = list(itertools.chain(*lst2))
     # Create counter
    counts = collections.Counter(all_words)
    max_value = max(counts.values())
    text_tokens = [ key for key, value in counts.items() if value < 0.7 *max_value ]
    return text_tokens


def write_to_file(dataset_name, template_dict):
    output_file = 'output/' + dataset_name + ".csv"
    with open(output_file, 'w') as csv_file:  
        writer = csv.writer(csv_file)
        writer.writerow(["template", "matches"])
        for key, value in template_dict.items():
            writer.writerow([key, value])
        logger.info(f"write to {output_file}")
    return output_file

def parse(logformat):
    ''' 
    Function to generate regular expression to split log messages
    '''
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(' +', '\\\s+', splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            regex += '(?P<%s>.*?)' % header
            headers.append(header)
    regex = re.compile('^' + regex + '$')
    return headers, regex

def read_input(input_file, logformat ,logpai_data):
    log_messages = []
    headers, regex = parse(logformat)
    with open(input_file, 'r') as f:
        for line in f:
            if logpai_data:
                line1 = line
            else:
                line1 = json.loads(line)["log"]
            try:
                match = regex.search(line1.strip())
                message = [match.group(header) for header in headers]
                log_messages.append(message)
            except Exception as e:
                logger.error(e)
    logdf = pd.DataFrame(log_messages, columns=headers)
    logdf['LineId'] = logdf.index + 1
    return logdf