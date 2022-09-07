import warnings
warnings.filterwarnings('ignore')
import sys
import pandas as pd
from collections import defaultdict
import scipy.special
import numpy as np
from nltk import ngrams

###### two line are indentical if the list of words is the same,+ the number of variable (<*>) 
# is greater or equal to the number of variable in the ground truth file
def compare(x,y):
    substring ="*"
   
    value= True
    comparison_column = []
    end = min(len(x), len(y))
    print(end)
    for line in range(0,end):
        words_x= re.split('\W+', str(x[line]))
        words_y= re.split('\W+', str(y[line]))
        list_x = [i for i in words_x if i]
        list_y = [i for i in words_y if i]

        list_y = [''.join(s.translate(string.punctuation)) for s in list_y]
        list_x = [''.join(s.translate(string.punctuation)) for s in list_x]
        list_x = [''.join(c for c in s if c not in string.punctuation) for s in list_x]
        list_y = [''.join(c for c in s if c not in string.punctuation) for s in list_y]
        #count_x = x[line].count(substring)
        #count_y = y[line].count(substring)
        '''
        if (set(list_y)!=(set(list_x))):
            print("====*********************************======")
            print(list_x)
            print(list_y)
        '''
        #comparison_column.append((list_x== list_y) and (count_x<=count_y))
        #comparison_column.append((set(list_y).issubset(set(list_x))) and (count_x<=count_y))
        if (not list_y):
            comparison_column.append(False)
        else:
            #comparison_column.append(((set(list_y).issubset(set(list_x)))))
            comparison_column.append((list_x== list_y))
    #print("true", comparison_column.count(True)) 
    #print("False", comparison_column.count(False)) 
    precision = comparison_column.count(True) / end
    print("precision= ",precision)
    #true positive (TP) decision assigns two log messages with the same log event to the same log group; 
    #a false positive (FP) decision assigns two log messages with different log events to the same log group; 
    #and a false negative (FN ) decision assigns two two log messages with the same log event to different log groups
    # recall = TP/TP+FN
    #recall = float(accurate_pairs) / real_pairs
    #f_measure = 2 * precision * recall / (precision + recall)
    return precision, comparison_column

def evaluate(groundtruth, parsedresult):
    """ Evaluation function to benchmark log parsing accuracy
    
    Arguments
    ---------
        groundtruth : str
            file path of groundtruth structured csv file 
        parsedresult : str
            file path of parsed structured csv file
    Returns
    -------
        f_measure : float
        accuracy : float
    """ 
    
    df_groundtruth = pd.read_csv(groundtruth)
    df_parsedlog = pd.read_csv(parsedresult)

    # pour windows
      #remove punctiation
    df_parsedlog['template'] = [' '.join(c for c in s if c not in string.punctuation) for s in df_parsedlog['template']]
    df_groundtruth['EventTemplate'] = [' '.join(c for c in s if c not in string.punctuation) for s in df_groundtruth['EventTemplate']]
    '''
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('<*>', '')
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('_', '')
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('<*>', '')
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('_', '')
    
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('_<*>', '')
    df_groundtruth['EventTemplate']= df_groundtruth['EventTemplate'].str.replace('_<*>', '')
    df_groundtruth['EventTemplate']= df_groundtruth['EventTemplate'].str.replace('<*', '')
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('<*', '')
    df_groundtruth['EventTemplate']= df_groundtruth['EventTemplate'].str.replace('<*>', '')
    df_parsedlog['new2']= df_parsedlog['new2'].str.replace('<*>', '')
    
    #remove punctiation
    df_parsedlog['new2'] = [' '.join(c for c in s if c not in string.punctuation) for s in df_parsedlog['new2']]
    df_groundtruth['EventTemplate'] = [' '.join(c for c in s if c not in string.punctuation) for s in df_groundtruth['EventTemplate']]

    df_parsedlog['new2']= df_parsedlog['new2'].str.replace(r'\[|\]', '')
    df_groundtruth['EventTemplate']= df_groundtruth['EventTemplate'].str.replace(r'\[|\]', '')
    '''
    toto = df_groundtruth.groupby(df_groundtruth.EventTemplate,as_index=False).size()
    print (toto)
    toto2 = df_groundtruth.groupby(df_parsedlog.template,as_index=False).size()
    table1= toto['size'].append(toto2['size'])
    table2= toto['EventTemplate'].append(toto2['template'])
    df = pd.DataFrame()
    #df_ = df_.fillna(0) # with 0s rather than NaNs
    df['event']=table2
    df['size']=table1
    #print(table1)
    #print(table2)
    #df['event'] =  df['event'].str.replace('[|]', '')
    df = df.sort_values("event",  ascending=False)

    df.to_csv("output/grouping.csv")
    #print(df)
    

    
    comparison_column = np.where(compare(df_groundtruth['EventTemplate'],df_parsedlog['template']), True, False)
    precision, df_parsedlog["equal"] = compare(df_groundtruth['EventTemplate'],df_parsedlog['template'])
    df_parsedlog["ground"] = df_groundtruth['EventTemplate']
    #df_groundtruth['EventTemplate'].to_csv('output/groundEventTemplate.csv')
    #df_parsedlog['event_label'].to_csv('output/parsedEventLabel.csv')
    df = df_parsedlog[['template','ground','equal']]
    mycsv= df.to_csv('output/evaluation.csv')
 
    #get_accuracy(df_groundtruth['EventTemplate'], df_parsedlog['new2'])
    #print('Precision: %.4f, Recall: %.4f, F1_measure: %.4f'%(precision, recall, f_measure))
    #return f_measure
    return precision


def measure_accuracy(ground, outputlog):
    #Process_Remove_Duplicates(logfile, formatting ,0.65,2000)
    return evaluate(ground,outputlog)