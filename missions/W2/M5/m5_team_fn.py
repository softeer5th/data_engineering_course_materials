import pandas as pd
def splitDataFrame(df:pd.DataFrame, cnt:int) -> list:
    res = []
    idx = 0
    split_size = df.shape[0] // cnt
    for i in range(1, cnt):
        res.append(
            df[idx:idx + split_size + 1]
        )
        idx += split_size + 1
    res.append(df[idx:])
    
    return res

def countWord(df:pd.DataFrame, target:str)->dict:
    # split
    word_df = df[df['teacher_name'] == target].loc[:,'text'].str.split()
    # 소문자
    word_exp_df = word_df.explode().str.lower()
    # to dict
    word_cnt_dict = word_exp_df.value_counts().to_dict()
    # 쓸데없는 단어 제거
    # for s_word in STOPWORDS:
    #     word_cnt_dict.pop(s_word, None)
    if word_cnt_dict.get('&amp;', 0) != 0:
        print("있따")
    return word_cnt_dict