from time import sleep
from multiprocessing import Pool

class Work:
    def __init__(self, name: str, duration: int):
        self.name = name
        self.duration = duration

def work_log(work: Work)-> None:
    print(f'Process {work.name} waiting {work.duration} seconds', flush=True)
    sleep(work.duration)
    print(f'Process {work.name} Finished.', flush=True)


if __name__ == '__main__':
    works = [Work('A', 5), Work('B', 2), Work('C', 1), Work('D', 3)]

    with Pool(2) as p:
        p.map(work_log, works)