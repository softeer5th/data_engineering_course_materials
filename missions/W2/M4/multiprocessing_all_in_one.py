import multiprocessing as mp
import time

# constants
TASK_COUNT = 4
EXECUTION_TIME = 0.5

def work(todo:mp.Queue, done:mp.Queue):
    while not todo.empty():
        # find task to do
        try:
            i = todo.get_nowait()   # by non-blocking
        except:
            continue    # if todo is empty: re-check is todo is empty
        else:
            print(f"Task no {i} at {mp.current_process().name}")
            time.sleep(EXECUTION_TIME)
            done.put((i, mp.current_process().name))
    #print(f"{mp.current_process().name} is done!")
    mp.current_process().close()

if __name__ == "__main__":
    # make todo Table
    task_to_accomplish = mp.Queue()
    # initialize todo Table
    for i in range(10):
        task_to_accomplish.put(i)
    
    # make done Table
    task_that_are_done = mp.Queue()

    # make processes
    proc = []
    for i in range(TASK_COUNT):
        p = mp.Process(target=work, args=(task_to_accomplish, task_that_are_done))
        p.start()
        proc.append(p)

    # wait for processes end
    for i in range(TASK_COUNT):
        proc[i].join()

    # print the done Table
    while not task_that_are_done.empty():
        taskNo, procName = task_that_are_done.get()
        print(f"Task no {taskNo} is done by {procName}")