# mapred.py
import logging
import re

from mrjob.job import MRJob

# 로깅 설정
logging.basicConfig(
    filename="mapper.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        logging.debug(f"Processing line: {line}")
        try:
            for word in WORD_RE.findall(line):
                word = word.lower()
                logging.debug(f"Yielding word: {word}")
                yield (word, 1)
        except Exception as e:
            logging.error(f"Error in mapper: {e}")

    def combiner(self, word, counts):
        logging.debug(f"Combining word: {word}")
        yield (word, sum(counts))

    def reducer(self, word, counts):
        logging.debug(f"Reducing word: {word}")
        yield (word, sum(counts))


if __name__ == "__main__":
    MRWordFreqCount.run()
