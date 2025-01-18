import time
from multiprocessing import Process, Manager, Lock

def do_work(tasks_to_accomplish, tasks_that_are_done, lock, worker_id):
    while True:
        task = None
        # 작업 가져오는 부분을 동기화
        with lock:
            if tasks_to_accomplish:
                task = tasks_to_accomplish.pop(0)  # 작업 가져오기
        if task is None:  # 작업이 없으면 종료
            print(f"Process-{worker_id} 작업 완료")
            break

        print(f"작업 번호 {task}")
        time.sleep(0.5)  # 작업 수행 시뮬레이션
        tasks_that_are_done.append(f"작업 번호 {task} : Process-{worker_id}에 의해 수행됨")  # 결과 저장


def main():

    start_time = time.time()  # 시작 시간 기록
    print(f"작업 시작: {time.ctime(start_time)}")

    manager = Manager()
    tasks_to_accomplish = manager.list(range(1000))  # 작업 목록 (공유 리스트)
    tasks_that_are_done = manager.list()  # 결과 저장용 리스트 (공유 리스트)
    lock = Lock()  # Lock 객체 생성

    processes = []
    for i in range(4):  # 4개의 프로세스 생성
        process = Process(target=do_work, args=(tasks_to_accomplish, tasks_that_are_done, lock, i + 1))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    end_time = time.time()  # 종료 시간 기록
    print(f"작업 종료: {time.ctime(end_time)}")

    # 결과 출력
    for result in tasks_that_are_done:
        print(result)
    
    # 실행 속도 출력
    print(f"총 실행 시간: {end_time - start_time:.2f}초")

# 실행
if __name__ == "__main__":
    main()