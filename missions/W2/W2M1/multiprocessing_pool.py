import time
from multiprocessing import Pool

# 작업 로그 함수 정의
def work_log(task):
    name, duration = task  # 작업 이름과 기간 추출
    print(f"프로세스 {name}가 {duration}초간 대기")
    time.sleep(duration)  # 지정된 시간 동안 작업 시뮬레이션
    print(f"프로세스 {name}가 완료됨.")


def main():
    tasks = [('A', 5), ('B', 2), ('C', 1), ('D', 3)]

    worker_pool = 2
    print(f"풀에 {worker_pool}개의 작업자를 할당")

    with Pool(worker_pool) as pool:
        pool.map(work_log, tasks)

    print("모든 작업 완료")

# 실행
if __name__ == "__main__":
    main()

