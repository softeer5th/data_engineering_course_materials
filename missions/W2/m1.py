import multiprocessing as mp
import time

#작업
def execute_task(task):
    name = task[0]
    duration = task[1]
    print(f'Process {name} waiting {duration} seconds')
    time.sleep(duration) 
    print(f'Process {name} finished')

# const block
NUM_OF_WORKERS = 2

# main
if __name__ == '__main__':
    tasks = [['A', 5], ['B', 2], ['C', 1], ['D', 3]]
    with mp.Pool(processes=NUM_OF_WORKERS) as pool:
        result = pool.map(execute_task, tasks)
