import sys

curr_word = None
curr_count = 0

for line in sys.stdin:
    line = line.strip()

    word, count = line.split("\t", 1)
    count = int(count)

    if curr_word == word:
        curr_count += count
    else:
        if curr_count:
            print(f"{curr_word}\t{curr_count}")
        curr_count = count
        curr_word = word

if curr_word == word:
    print(f"{curr_word}\t{curr_count}")
