import multiprocessing
from multiprocessing import Process

def work_log(task='아시아'):
    '''
    Args: 
        task(str)
    '''
    # print(f"PID Child   : {multiprocessing.current_process().pid}")
    print(f"Proc Name   : {multiprocessing.current_process().name}")
    print(f"PID Parent  : {multiprocessing.parent_process().pid}")
    print(f'대륙의 이름은 : {task}')

tasks = ['아메리카', '유럽', '아프리카']

if __name__ == "__main__":
    num_processes = 2
    
    proc1 = Process(target=work_log)
    processes = [Process(target=work_log, args=(task, )) for task in tasks]
    
    proc1.start()
    for proc in processes:
        proc.start()
        
    proc1.join()
    for proc in processes:
        proc.join()