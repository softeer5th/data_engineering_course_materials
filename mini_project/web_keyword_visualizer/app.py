from calculator import calc_distances
from visualizer import visualize_word_distances


def combine_distances(all_distances_list):
    word_distances = {}
    for distances in all_distances_list:
        for word, distance in distances:
            if word not in word_distances:
                word_distances[word] = []
            word_distances[word].append(distance)

    avg_distances = [
        (word, sum(distances)) for word, distances in word_distances.items()
    ]
    return sorted(avg_distances, key=lambda x: x[1], reverse=True)


if __name__ == "__main__":

    keyword = "컨테이너"

    all_distances = []
    for i in range(1, 6):
        with open(f"text/{i}.txt", "r") as f:
            text = f.read()
            distances = calc_distances(text, keyword)
            all_distances.append(distances)

    avg_sorted_distances = combine_distances(all_distances)
    visualize_word_distances(keyword, avg_sorted_distances[:100])
