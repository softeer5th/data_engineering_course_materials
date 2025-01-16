import multiprocessing as mp
import time
from functools import partial

# constants
THREAD_COUNT = 4
CONTINENTS = ["Africa", "America", "Europe", None]

def printNameContinent(lock, name:str):
    # with the lock, write log that the process is started
    lock.acquire()
    with open("./M2Log.txt", "a") as f:
        f.write(f"{mp.current_process().pid} start, arg='{name}'\n")
    lock.release()

    # main process of this function
    print(name if name else 'Asia')
    # time.sleep(1) # simulate process time

    # with the log, write log that the process is end
    lock.acquire()
    with open("./M2Log.txt", "a") as f:
        f.write(f"{mp.current_process().pid} end\n")
    lock.release()

if __name__ == "__main__":
    # make a Lock
    m = mp.Manager()
    l = m.Lock()
    
    # make processes
    procs = []
    for i in range(len(CONTINENTS)):
        p = mp.Process(
            target=printNameContinent,
            args=(l, CONTINENTS[i])
        )
        p.start()
        procs.append(p)
    
    # wait for processes
    for p in procs:
        p.join()
