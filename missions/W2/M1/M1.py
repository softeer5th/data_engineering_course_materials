import multiprocessing
import time

def work_log(task):
    '''
    Args: 
        task(str, int)
    '''
    name, wait = task
    print(f"프로세스 {name}가 {wait}초간 대기")
    time.sleep(wait)
    print(f"프로세스 {name}가 완료됨.")

tasks = { # task_name : duration(sec)
    'A' : 5,
    'B' : 2,
    'C' : 1,
    'D' : 3
} 

if __name__ == "__main__":

    with multiprocessing.Pool(processes=2) as pool:
        pool.map(work_log, list(tasks.items()))