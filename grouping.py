from re import template
import pandas as pd
import regex as re 
# import re
import nltk
from nltk.corpus import words 
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import datetime
import os
#from textpack import tp
# from string_grouper import match_strings, match_most_similar, group_similar_strings, compute_pairwise_similarities, StringGrouper

# K-MEANS CLUSTERING
# Importing Modules
from sklearn import datasets
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN
import itertools
import warnings
from collections import Counter, defaultdict
from string import punctuation
from utils import write_to_file, read_input

from ulp_tokenize import mytokenize
warnings.filterwarnings("ignore")    

import logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__file__)
logger.setLevel("DEBUG")


def create_dataframe(matrix, tokens):
    return pd.DataFrame(data=matrix, columns=tokens)


def trans(string):
    string =' '.join(dict.fromkeys(string.split()))
    return filter(None, (word.strip(punctuation) for word in string.split()))

def getDynamicVars2(petit_group) :
    #values=[]
    #length  =  petit_group.size
    petit_group['tokenized_log'] = petit_group['tokenized_log'].map(lambda x:' '.join(dict.fromkeys(x.split())))
    petit_group['tokenized_log'] = petit_group['tokenized_log'].map(lambda x:' '.join(filter(None, (word.strip(punctuation) for word in x.split()))))

    lst = petit_group["tokenized_log"].values.tolist()
    # logger.info(lst)
    vec =[]
    big_lst = ' '.join(v for v in lst)
    #big_list = [word for line in lst for word in line.split()]
    this_count = Counter(big_lst.split())
    if (this_count):
        max_val = max (this_count, key=this_count.get)
        for word in this_count:
            if this_count[word] < this_count[max_val] :
                vec.append(word)

    return vec

def remove_word_with_special(sentence):
    sentence = sentence.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,/<>?\|`~-=+"})
    # sentence = sentence.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,<>?\|`~-=+"})
    length = len(splitted_sentence:=sentence.split())
    finale=""
    for word in splitted_sentence:
        if len(word) > 1 and all(c.isalpha() for c in word):
            finale += word

    finale= finale+str(length)
    return finale

def get_only_letters(sentence):
    return ''.join([i for i in sentence.split() if not i.isdigit()])
    # return ''.join([c for c in sentence.split() if c.isalpha()])


def template_cleaning(groups):
    template_dict = defaultdict(list)
    count= 0    
    re_list2 = ["[ ]{1,}[-]*[0-9]+[ ]{1,}", " \"\d+\" "]
    generic_re = re.compile( '|'.join( re_list2))

    print(len(groups.groups.keys()))
    for i in (keys:=groups.groups.keys()):
        l=[]
        slc = groups.get_group(i)
        template = slc['tokenized_log'][0:1].to_list()[0]
        ids = list(slc["LineId"].values)
        count+=1
        old_template = template
        if  (slc.size>1 ):
            l  = getDynamicVars2(slc)
            
            dynamic_token_pat = r'\b(?:{})\b'.format('|'.join(str(v) for v in l))
            if (len(l)>0):
                template = re.sub(dynamic_token_pat,'<*>', template)
                
        # old_template = template
        template = re.sub(generic_re, " <*> ", template)
        template_dict[template].extend(ids)
        # if old_template != template:
        #     print(f"changed template : \n{old_template}\n {template}")

    return template_dict


def string_similar(str1, str2):
    '''
    let's say 2 strings with atmost x(x = 1) token diffirent is 'similar', which means they are likely belongs to the same template
    '''
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

def template_merge(template_dict):
    template_list = list(template_dict.keys())
    
    first_template = template_list[0]
    ite = 1
    while(ite < len(template_list)):
        is_similar, merged_tokens = string_similar(first_template, this_template:=template_list[ite])
        if is_similar:
            # then we should merge these 2, which is already done. BUT NEEDS IMPROVEMENT
            merged_template = " ".join(merged_tokens)
            for t in [first_template, this_template]:
                if merged_template != t:                  
                    template_dict[merged_template].extend(template_dict[t])
                    template_dict.pop(t)
            first_template = merged_template
        else:
            first_template = this_template
        ite += 1 

    return template_dict


def log_parsing(dataset_name, logformat, similarity_score, logpai_data=True):
    '''
    main entry
    '''
    if logpai_data:
        input_file = "logs/" + dataset_name + "/" + dataset_name + "_2k.log"
    else: ## opni data
        input_file = "opni-input/" + dataset_name
    logdf = read_input(input_file ,logformat, logpai_data)   
    logdf = mytokenize(logdf)

    logdf['masked_log'] = logdf['tokenized_log'].map(lambda x:remove_word_with_special(str(x)))
    groups = logdf.groupby('masked_log')
    # keys: example: INFOnodecontrollerranchermachineCreatingNewSSHKey7

    template_dict = template_cleaning(groups)
    # template_dict = template_merge(template_dict)
    
    output_file = write_to_file(dataset_name, template_dict)
    return  output_file
