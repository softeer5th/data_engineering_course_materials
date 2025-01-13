import multiprocessing as mp
from multiprocessing import Pool
import time

class Task:
    def __init__(self, name:str, time:int):
        self.name = name
        self.time = time
    
def work(task:Task):
    print(f'Process {task.name} waiting {task.time} seconds')
    time.sleep(task.time)
    print(f'Process {task.name} Finished.')
    return

if __name__ == "__main__":
    task_list = [Task('A', 5), Task('B', 2), Task('C', 1), Task('D', 3)]
    workers = 2
    pool = Pool(workers)
    pool.map(work, task_list)