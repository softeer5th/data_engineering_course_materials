from multiprocessing import Pool
import time

WORKING_SET = [
    ("A", 5),
    ("B", 2),
    ("C", 1),
    ("D", 3),
]


def work_log(work):
    print(f"Process {work[0]} waiting {work[1]} seconds")
    time.sleep(work[1])
    print(f"Process {work[0]} finished")


def main():
    with Pool(2) as pool:
        pool.map(work_log, WORKING_SET)


if __name__ == "__main__":
    main()
