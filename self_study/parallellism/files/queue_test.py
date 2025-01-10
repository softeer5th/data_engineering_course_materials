import multiprocessing as mp
import random
import time
from queue import Empty


def process_task(queue: mp.Queue, process_id: int) -> None:
    """
    큐에서 데이터를 가져와 처리하는 프로세스 함수
    """
    while True:
        # 큐에서 데이터를 가져옴 (1초 타임아웃)
        data = queue.get()

        if data is None:
            # None이면 종료
            print(f"프로세스 {process_id}: 작업 완료")
            break

        # 처리 시작을 알림
        print(f"프로세스 {process_id}: 값 {data} 처리 시작")

        # 데이터 값/10 초 만큼 대기
        time.sleep(data / 10)

        # 처리 완료를 알림
        print(f"프로세스 {process_id}: 값 {data} 처리 완료")


def main():
    # 프로세스 간 공유할 큐 생성
    task_queue = mp.Queue()

    # 1~10 사이의 랜덤한 값 10개를 큐에 넣기
    numbers = random.sample(range(1, 11), 10)
    numbers.extend([None] * 4)
    for num in numbers:
        task_queue.put(num)

    # 4개의 프로세스 생성
    processes = []
    for i in range(4):
        p = mp.Process(target=process_task, args=(task_queue, i))
        processes.append(p)
        print(f"프로세스 {i} 시작")
        p.start()

    # 모든 프로세스가 종료될 때까지 대기
    print("모든 프로세스가 종료될 때까지 대기합니다.")
    for p in processes:
        print(f"프로세스 {p.pid} 대기 중")
        p.join()

    print("모든 작업이 완료되었습니다.")


if __name__ == "__main__":
    main()
