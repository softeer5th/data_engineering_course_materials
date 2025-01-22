import re
import sys

WORD_RE = re.compile(r"[\w']+")

for line in sys.stdin:
    line = line.strip().lower()
    words = WORD_RE.findall(line)

    for word in words:
        print(f"{word}\t1")
