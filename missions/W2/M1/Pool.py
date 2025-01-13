from multiprocessing import Pool 
import time

class Task :
    def __init__(self, name, duration):
        self.name = name 
        self.duration = duration

def work_log(task : Task) : 
    print('Process {name} wating {time} seconds'.format(name = task.name, time = task.duration))
    time.sleep(task.duration)
    print('Process {name} Finished.'.format(name = task.name))

if __name__ == "__main__":
    numberOfWorkers = 2
    with Pool(processes=numberOfWorkers) as pool:
        tasks = [
            Task("A", 5), Task("B", 2), Task("C",1), Task("D",3)
        ]
        pool.map(work_log, tasks)