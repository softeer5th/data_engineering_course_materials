#!/usr/bin/python3
import sys

def reducer():
    current_word = None
    current_count = 0
    word = None

    for line in sys.stdin:
        line = line.strip()
        word, count = line.split("\t")
        count = int(count)

        if current_word == word:
            current_count += count
        else:
            if current_word:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    if current_word == word:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    reducer()
