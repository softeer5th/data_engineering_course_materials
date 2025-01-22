from mrjob.job import MRJob


class MyMRJob(MRJob):
    def mapper(self, _, line):
        """
        첫번째 인자는 line 번호, 두번째 인자는 line 내용
        """
        words = line.split()
        for word in words:
            yield word, 1

    def combiner(self, word, counts):
        """
        mapper에서 얻은 결과를 중간 집계한다.
        실행이 항상 보장되는 것은 아니므로
        연산에 대한 교환/결합 법칙이 성립하는지 잘 확인해야 한다.
        """
        yield word, sum(counts)

    def reducer(self, word, counts):
        """
        일단 combiner와 동일한 내용을 채웠다..
        """
        yield word, sum(counts)


if __name__ == "__main__":
    MyMRJob.run()
