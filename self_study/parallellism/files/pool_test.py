import time
from multiprocessing import Pool


def slow_square(x):
    time.sleep(1)  # 작업이 오래 걸리는 것을 시뮬레이션
    return x * x


def main():
    numbers = [1, 2, 3, 4, 5]

    print("일반적인 처리 시작")
    start = time.time()
    regular_result = [slow_square(x) for x in numbers]
    print(f"일반 처리 결과: {regular_result}")
    print(f"소요 시간: {time.time() - start:.2f}초\n")

    print("병렬 처리 시작")
    start = time.time()
    with Pool() as pool:
        parallel_result = pool.map(slow_square, numbers)
    print(f"병렬 처리 결과: {list(parallel_result)}")
    print(f"소요 시간: {time.time() - start:.2f}초")


if __name__ == "__main__":
    main()
