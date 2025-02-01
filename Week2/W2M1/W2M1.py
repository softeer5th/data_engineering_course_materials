import time
from multiprocessing import Pool, current_process

def work_log(task):
    task_name, duration = task  
    print(f"Process {task_name} waiting {duration} seconds")
    time.sleep(duration)  
    print(f"Process {task_name} Finished.")

if __name__ == "__main__":
    tasks = [("A", 5), ("B", 2), ("C", 1), ("D", 3)]
    
    with Pool(processes=2) as pool:
        pool.map(work_log, tasks)  # 입력 데이터를 여러 프로세스에 분산하고 각각의 프로세스에서 주어진 함수를 수행한 후 결과를 수집하는 역할