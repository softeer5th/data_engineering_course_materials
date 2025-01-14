from multiprocessing import Process, Queue, current_process
import time

def work(todo:Queue, done:Queue):
    while not todo.empty():
        data = todo.get()
        done.put(f'Task no {data} is done by {current_process().name}')
        time.sleep(0.5)
    return

if __name__ == '__main__':
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()

    for i in range(0, 10):
        print(f'Task no {i}')
        tasks_to_accomplish.put(i)

    pList = []
    for i in range(4):
        pList.append(Process(target=work, args=[tasks_to_accomplish, tasks_that_are_done]))

    for p in pList:
        p.start()

    for p in pList:
        p.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())