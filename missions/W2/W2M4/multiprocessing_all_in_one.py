import time
from multiprocessing import Process, Queue

def do_work(tasks_to_accomplish, tasks_that_are_done, worker_id):
    while True:
        try:
            task = tasks_to_accomplish.get_nowait()
        except Exception as e:
            print(f"Process-{worker_id} 작업 완료")
            break
        print(f"작업 번호 {task}")
        # 작업 완료 메시지 입력
        time.sleep(0.5)
        tasks_that_are_done.put(f"작업 번호 {task} : Process-{worker_id}에 의해 수행됨")
        

def main():
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()

    for i in range(100):
        tasks_to_accomplish.put(i)

    processes = []
    for i in range(4):
        process = Process(target=do_work, args=(tasks_to_accomplish, tasks_that_are_done, i+1))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    # 결과 출력
    while not tasks_that_are_done.empty():  # 큐가 비어있지 않을 때까지
        print(tasks_that_are_done.get())  # 큐에서 결과 꺼내기

# 실행
if __name__ == "__main__":
    main()