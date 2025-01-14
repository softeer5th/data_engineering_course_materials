from multiprocessing import Process, Queue, current_process
import time

#tasks_to_accomplish에서 작업을 검색, 실행, tasks_that_are_done에 결과를 저장
def worker(tasks_to_accomplish, tasks_that_are_done):
    while True:
        try:
            # 큐에서 작업 가져오기
            task = tasks_to_accomplish.get_nowait()  # 비어있을 때 예외 발생
        except:
            break  # 큐가 비어있으면 루프 종료

        print(f"Task no {task}")
        time.sleep(0.5)  # 작업 실행 시간
        result = f"Task no {task} is done by {current_process().name}"
        tasks_that_are_done.put(result)  # 완료메시지 큐에 저장


if __name__ == "__main__":
    tasks_to_accomplish = Queue()  # 작업 큐
    tasks_that_are_done = Queue()  # 완료메시지 큐

    # 작업 큐에 작업 분배
    for i in range(10):
        tasks_to_accomplish.put(i)

    # 4개의 프로세스 생성 - worker()함수 병렬로 실행
    # 프로세스들은 두 큐를 공유한다
    processes = []
    for i in range(4):
        process = Process(target=worker, args=(tasks_to_accomplish, tasks_that_are_done))
        processes.append(process)
        process.start()  # 프로세스 시작

    # 모든 프로세스가 작업을 마칠 때까지 대기
    for process in processes:
        process.join()
    
    # 완료메시지 큐에서 완료 메시지 출력
    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())

    # 큐 닫고 관련 스레드 종료 대기
    tasks_to_accomplish.close()  
    tasks_that_are_done.close()  
    tasks_to_accomplish.join_thread()
    tasks_that_are_done.join_thread()
