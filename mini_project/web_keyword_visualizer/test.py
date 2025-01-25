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


def extract_meaningful_words(text):
    """
    텍스트에서 한국어와 영어의 명사, 동사, 형용사, 부사를 추출하여 인덱스와 함께 리스트로 반환합니다.

    Args:
        text: 분석할 텍스트 (str)

    Returns:
        list[(word, position)]: 단어와 텍스트 내 시작 인덱스를 튜플로 묶은 리스트
    """

    okt = Okt()
    stop_words = set(stopwords.words("english"))  # 영어 불용어
    result = []

    # 한글, 영어, 숫자, 공백만 남기고 다 제거
    text = re.sub(r"[^가-힣A-Za-z0-9\s]", "", text)

    # 한국어와 영어 텍스트 분리 (간단한 방법 사용)
    korean_text = ""
    english_text = ""
    for char in text:
        if "가" <= char <= "힣":
            korean_text += char
        elif "A" <= char <= "z":
            english_text += char
        else:
            korean_text += " "
            english_text += " "

    # 한국어 형태소 분석
    if korean_text.strip():
        pos_result = okt.pos(korean_text, norm=True, stem=True)
        for word, pos in pos_result:
            if pos in ["Noun", "Verb", "Adjective", "Adverb"]:
                # 원본 텍스트에서 단어의 위치 찾기
                start_index = text.find(word)
                if start_index != -1:
                    result.append((word, start_index))
                    # 찾은 단어는 공백으로 바꿔서 중복 방지
                    text = text.replace(word, " " * len(word), 1)

    # 영어 형태소 분석 및 품사 태깅
    if english_text.strip():
        tokens = word_tokenize(english_text)
        pos_result = pos_tag(tokens)
        for word, pos in pos_result:
            if (
                pos.startswith("NN")
                or pos.startswith("VB")
                or pos.startswith("JJ")
                or pos.startswith("RB")
            ):
                # 불용어 제외 및 소문자 변환
                word_lower = word.lower()
                if word_lower not in stop_words:
                    # 원본 텍스트에서 단어의 위치 찾기
                    start_index = text.find(word)
                    if start_index != -1:
                        result.append((word_lower, start_index))
                        # 찾은 단어는 공백으로 바꿔서 중복 방지
                        text = text.replace(word, " " * len(word), 1)

    return result


with open("sample.txt", "r", encoding="utf-8") as f:
    text = f.read()

all_words = extract_meaningful_words(text)


all_words_unique = list(set([word for word, _ in all_words]))  # 중복 제거

# 3. 단어 위치 기반 거리 계산 함수 (수정 불필요)


def calculate_distances_by_position(keyword, word_list, all_words):
    distances = {}
    keyword_positions = [pos for word, pos in all_words if word == keyword]

    if not keyword_positions:
        return distances

    for word in word_list:
        if word != keyword:
            word_positions = [pos for w, pos in all_words if w == word]
            all_distances = []  # 모든 거리 저장
            for kp in keyword_positions:
                for wp in word_positions:
                    distance = abs(kp - wp)
                    all_distances.append(distance)  # 모든 거리 추가

            # 평균 거리 계산 및 저장
            avg_distance = np.mean(all_distances)
            distances[word] = 1 / (avg_distance + 1)

    return distances


# 4. 기준 키워드 설정 및 거리 계산 (영어 키워드로 변경)
keyword = "손흥민"  # 원하는 영어 키워드로 변경
distances = calculate_distances_by_position(
    keyword, all_words_unique, all_words
)

# 5. 거리 순으로 정렬
sorted_distances = sorted(
    distances.items(), key=lambda x: x[1], reverse=True
)  # 내림차순 정렬


def visualize_word_distances(keyword, sorted_distances):
    # 한글 폰트 설정
    font_path = "/System/Library/Fonts/AppleSDGothicNeo.ttc"
    fontprop = fm.FontProperties(fname=font_path)

    G = nx.Graph()
    G.add_node(keyword)

    # 노드 위치 계산
    pos = {keyword: np.array([0, 0])}  # 키워드는 중심에

    # 각 단어를 거리에 비례하여 원형으로 배치
    num_words = len(sorted_distances)
    for i, (word, distance) in enumerate(sorted_distances):
        G.add_node(word)
        G.add_edge(keyword, word, weight=distance)

        # 거리의 역수를 실제 거리로 변환 (1/(d+1) -> d)
        actual_distance = (1 / distance - 1) * 300  # 스케일 조정
        angle = 2 * np.pi * i / num_words * 20
        x = actual_distance * np.cos(angle)
        y = actual_distance * np.sin(angle)
        pos[word] = np.array([x, y])

    edge_colors = [d["weight"] for (u, v, d) in G.edges(data=True)]

    plt.figure(figsize=(10, 10))
    nx.draw(
        G,
        pos,
        with_labels=True,
        node_size=2000,
        node_color="skyblue",
        edge_color=edge_colors,
        width=2.0,
        edge_cmap=plt.cm.Blues,
        font_family=fontprop.get_name(),
        font_size=10,
    )
    plt.title(f"'{keyword}'와의 거리 시각화", fontproperties=fontprop)
    plt.show()


visualize_word_distances(keyword, sorted_distances)
