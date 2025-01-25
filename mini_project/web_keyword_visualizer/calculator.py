import re

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import networkx as nx
import nltk
import numpy as np
import pandas as pd
from konlpy.tag import Okt
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize

# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')


def extract_meaningful_words(text, keyword=None):
    okt = Okt()
    korean_stop_words = {
        "의",
        "가",
        "이",
        "은",
        "는",
        "을",
        "를",
        "에",
        "와",
        "과",
        "도",
    }
    english_stop_words = set(stopwords.words("english"))

    if keyword:
        korean_stop_words.discard(keyword)
        english_stop_words.discard(keyword.lower())

    result = []
    text = re.sub(r"[^가-힣A-Za-z0-9\s]", "", text)

    # 키워드 처리
    if keyword:
        keyword_positions = [m.start() for m in re.finditer(keyword, text)]
        for pos in keyword_positions:
            result.append((keyword, pos))
        text = text.replace(keyword, " " * len(keyword))

    # 한글/영어 분리
    korean_text = "".join(
        char if "가" <= char <= "힣" else " " for char in text
    )
    english_text = "".join(
        char if "A" <= char <= "z" else " " for char in text
    )

    # 외래어 추정 패턴
    def is_likely_loanword(word):
        if len(word) >= 2 and re.match(r"^[가-힣]+$", word):
            if not any(
                word.endswith(x)
                for x in ["은", "는", "이", "가", "을", "를", "의"]
            ):
                return True
        return False

    # 한글 처리
    if korean_text.strip():
        pos_result = okt.pos(korean_text, norm=True, stem=True)
        current_word = ""

        for i, (word, pos) in enumerate(pos_result):
            if pos == "Noun":
                if current_word:
                    if is_likely_loanword(current_word):
                        start_index = text.find(current_word)
                        if start_index != -1:
                            result.append((current_word, start_index))
                            text = text.replace(
                                current_word, " " * len(current_word), 1
                            )
                    current_word = ""

                current_word = word

                # 마지막 단어 처리
                if i == len(pos_result) - 1:
                    if is_likely_loanword(current_word):
                        start_index = text.find(current_word)
                        if start_index != -1:
                            result.append((current_word, start_index))
                            text = text.replace(
                                current_word, " " * len(current_word), 1
                            )
            else:
                if current_word and is_likely_loanword(current_word):
                    start_index = text.find(current_word)
                    if start_index != -1:
                        result.append((current_word, start_index))
                        text = text.replace(
                            current_word, " " * len(current_word), 1
                        )
                current_word = ""

                if (
                    pos in ["Verb", "Adjective"]
                    and word not in korean_stop_words
                ):
                    start_index = text.find(word)
                    if start_index != -1:
                        result.append((word, start_index))
                        text = text.replace(word, " " * len(word), 1)

    # 영어 처리
    if english_text.strip():
        tokens = word_tokenize(english_text)
        pos_result = pos_tag(tokens)
        for word, pos in pos_result:
            if pos.startswith(("NN", "VB", "JJ", "RB")):
                word_lower = word.lower()
                if word_lower not in english_stop_words:
                    start_index = text.find(word)
                    if start_index != -1:
                        result.append((word_lower, start_index))
                        text = text.replace(word, " " * len(word), 1)

    return result


def calculate_distances_by_position(keyword, word_list, all_words):
    distances = {}
    keyword_positions = [pos for word, pos in all_words if word == keyword]

    for word in word_list:
        if word != keyword:
            word_positions = [pos for w, pos in all_words if w == word]
            all_distances = []
            for kp in keyword_positions:
                for wp in word_positions:
                    distance = abs(kp - wp)
                    all_distances.append(1 / (distance + 1))
            distances[word] = sum(all_distances)
    return distances


def calc_distances(text, keyword):
    all_words = extract_meaningful_words(text, keyword)
    all_words_unique = list(set([word for word, _ in all_words]))
    distances = calculate_distances_by_position(
        keyword, all_words_unique, all_words
    )
    return sorted(distances.items(), key=lambda x: x[1], reverse=True)
