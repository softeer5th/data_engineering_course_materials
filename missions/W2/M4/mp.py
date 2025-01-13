from multiprocessing import Process, Queue
from time import sleep

def task(tq, rq, pid):
    while True:
        try:
            tid = tq.get_nowait()
        except:
            break
        print(f'Task no {tid}')
        sleep(0.5)
        rq.put(f"Task no {tid} is done by Process-{pid}")
    
if __name__ == '__main__':
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()

    for tid in range(10):
        tasks_to_accomplish.put(tid)

    processes = []
    for pid in range(1, 5):
        p = Process(target=task, args=(tasks_to_accomplish, tasks_that_are_done, pid,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())