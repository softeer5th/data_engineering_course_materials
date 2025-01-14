import pandas as pd
import nltk
import time
from nltk.tokenize import word_tokenize

def myFilter(s:str)-> str:
    if s[0] == '@':
        return ''
    elif len(s) == 1:
        return ''
    else:
        return s.strip("!@#$%^&*()-=;'\",.")

def mySplit(text:str)->list:
    res = []
    res = list(filter(lambda x: x != '', map(myFilter, (text.split()))))
    return res

if __name__ == '__main__':
    s = time.time()
    path = './missions/W2/M5/data/'
    data_name = 'training.1600000.processed.noemoticon.csv'
    full_name = path + data_name
    df = pd.read_csv(full_name, usecols=['text'], nrows=10)
    e = time.time()
    print(df['text'].apply(mySplit))
    print(f'elapsed: {s - e}')