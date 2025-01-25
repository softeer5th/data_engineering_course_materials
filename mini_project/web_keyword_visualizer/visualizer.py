import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd


def visualize_word_distances(keyword, sorted_distances):
    font_path = "/System/Library/Fonts/AppleSDGothicNeo.ttc"
    fontprop = fm.FontProperties(fname=font_path)

    G = nx.Graph()
    G.add_node(keyword)
    pos = {keyword: np.array([0, 0])}

    num_words = len(sorted_distances)
    for i, (word, distance) in enumerate(sorted_distances):
        G.add_node(word)
        G.add_edge(keyword, word, weight=distance)

        # 거리가 크면(관계가 강하면) 더 가까이 배치
        actual_distance = 100 / (distance + 1)  # 역수 관계 유지
        angle = 2 * np.pi * i / num_words * 3
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
