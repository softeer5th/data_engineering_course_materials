import multiprocessing as mp
from queue import Empty, Full
import time

# queue를 이용하여 프로세스 간 공유되는 작업 리스트를 만듦.
# 함수에서 queue를 이용하여 작업을 받아오고, 
# 작업이 끝나면 다른 queue에 작업이 끝났다는 메시지를 넣음.
def execute_task(
        pid: int, 
        tasks_to_accomplish: mp.Queue, 
        tasks_that_are_done: mp.Queue
    ):
    while True:
        try:
            # queue가 비어있을 때까지 받아옴.
            task_name = tasks_to_accomplish.get_nowait() # non-blocking
            if task_name is None:
                break
            print(f"Task no {task_name}")
            time.sleep(0.5)
            tasks_that_are_done.put(f"Task no {task_name} is done by Process-{pid}")
        except Empty:
            # queue가 비어있으면 프로세스 종료
            print(f"Process-{pid} is empty.")
            pass
        except Full:
            print(f"Process-{pid} is full.")
            pass


def main():
    tasks_to_accomplish = mp.Queue()
    tasks_that_are_done = mp.Queue()

    for i in range(10):
        tasks_to_accomplish.put(i)
    tasks_to_accomplish.put(None)
    
    processes = [mp.Process(target=execute_task, 
                            args=(i, tasks_to_accomplish, tasks_that_are_done)) 
                            for i in range(1, 5)]
    
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())


if __name__ == "__main__":
    mp.set_start_method('spawn')
    main()