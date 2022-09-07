# from collections import defaultdict
from difflib import SequenceMatcher
import regex as re 
import sys
from collections import defaultdict
from math import *
from masker import LogMasker


masker = LogMasker()
def mytokenize(df_log): 
    tokenized_log = []
    for idx, log in df_log['Content'].iteritems():

        # test
        # log = masker.mask(log)

        tokens =log.split()

        tokens = re.sub(r"\\", "", str(tokens))
        tokens = re.sub(r"\'", "", str(tokens))
        # tokens = tokens.translate ({ord(c): "" for c in "!@#$%^&*{}<>?\|`~"})
        tokens = tokens.translate ({ord(c): "" for c in "!@#$%^&*{}?\|`~"})

        match4 = None
        # regex for mac address, hexadecimals, ip adress, time and long date
        # no more folder ==> removed
        # date  "\d{4}-\d{2}-\d{2}" for 2002-03-24 and "\d{4}\/\d{2}\/\d{2}" for 2002/03/24
        # mac == ([\da-fA-F]{2}:){5}[\da-fA-F]{2}
        # time [0-9]{2}:[0-9]{2}:[0-9]{2}  , long format "^(?:[0-9]{4}-[0-9]{2}-[0-9]{2})(?:[ ][0-9]{2}:[0-9]{2}:[0-9]{2})?(?:[.,][0-9]{3})?", short 
        # hexa 0[xX][0-9a-fA-F]+
        # http adress and http : https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)" , 
        #"http?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
        # folder "(\/|)([a-zA-Z0-9-]+\.){2,}([a-zA-Z0-9-]+)?(:[a-zA-Z0-9-]+|)(:|)" 
        #print(tokens)
        
        re_list = [ "([\da-fA-F]{2}:){5}[\da-fA-F]{2}","\d{4}-\d{2}-\d{2}", "\d{4}\/\d{2}\/\d{2}",
                   "[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.,][0-9]{3})?", "[0-9]{2}:[0-9]{2}:[0-9]{2}", "[0-9]{2}:[0-9]{2}", "0[xX][0-9a-fA-F]+", 
                   "([\(]?[0-9a-fA-F]*:){8,}[\)]?" , "^(?:[0-9]{4}-[0-9]{2}-[0-9]{2})(?:[ ][0-9]{2}:[0-9]{2}:[0-9]{2})?(?:[.,][0-9]{3})?",
                   "(\/|)([a-zA-Z0-9-]+\.){2,}([a-zA-Z0-9-]+)?(:[a-zA-Z0-9-]+|)(:|)" ]
        pat = r'\b(?:{})\b'.format('|'.join(str(v) for v in re_list))
        tokens = re.sub(pat, "<*>", str(tokens)) 
        # tokens = re.sub(pat, "<*>", " ".join(tokens)) 
        tokens = tokens.replace('=', ' = ').replace(')', ' ) ').replace('(', ' ( ').replace(']', ' ] ').replace('[', ' [ ').lower()
        tokenized_log.append(str(tokens).lstrip().replace(","," "))

    df_log["tokenized_log"] = tokenized_log
    return df_log