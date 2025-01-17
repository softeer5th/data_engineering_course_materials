from multiprocessing import Pool
import time

task_list = [['A',5], ['B',2],['C',1], ['D',3]]

def work_log(task_name, duration):
    print(f'Process {task_name} waiting {duration} seconds.')
    time.sleep(duration)
    print(f'Process {task_name} finished.')


if __name__ == '__main__':

    pool = Pool(2)
    inputs=task_list

    results = pool.starmap(work_log,inputs)
    pool.close() # or p.terminate()
    pool.join()
