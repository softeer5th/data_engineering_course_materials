import re
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer

from multiprocessing import Pool
from functools import partial

TEXT_CLEANING_RE_ALL = "@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+"
stop_words = stopwords.words("english")
stemmer = SnowballStemmer("english")


def clean_text(text):
    return re.sub(TEXT_CLEANING_RE_ALL, ' ', str(text).lower()).strip()

import time
def preprocess(text, stem=False):
    text = clean_text(text)
    tokens = []
    for token in text.split():
        if token not in stop_words:
            if stem:
                tokens.append(stemmer.stem(token))
            else:
                tokens.append(token)
    return " ".join(tokens)

def apply_preprocess(df_chunk):
    start_time = time.perf_counter()

    df_chunk.text = df_chunk.text.apply(lambda x: preprocess(x, True))
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"preprocess() took {elapsed_time:.6f} seconds.")
    return df_chunk

def parallel_apply(df, func, n_cores=None):
    if not n_cores:
        n_cores = cpu_count()

    data_split = np.array_split(df, n_cores)

    pool_func = partial(func)

    with Pool(n_cores) as pool:
        print(pool, len(data_split))
        data = pd.concat(pool.map(pool_func, data_split))

    return data

def main(df: pd.DataFrame):
    return parallel_apply(df, apply_preprocess)