import multiprocessing as mp
from multiprocessing import Process, Queue
# 내 코드 : args가 dict type
def push_work(task:tuple, queue:Queue):
    '''
    Args: 
        task (tuple(int, str))
        queue (multiprocess.Queue)
    '''
    num, color = task
    print(f'항목 번호: {num} {color}')
    # queue.put(f'항목 번호: {num} {color}')
    queue.put((num, color))
    
def pull_work(queue:Queue):
    '''
    Args: 
        queue (multiprocess.Queue)
    '''
    num, color = queue.get()
    print(f'항목 번호: {num-1} {color}')

tasks = { # task_name : duration(sec)
    1 : '빨간색',
    2 : '녹색',
    3 : '파란색',
    4 : '검정색'
}

colors = ['빨간색', '녹색', '파란색', '검정색']

if __name__ == "__main__":
    
    queue = Queue()
    proc_push = [Process(target=push_work, args=(task, queue))for task in tasks.items()]
    proc_pull = [Process(target=pull_work, args=(queue,))for _ in range(len(tasks.keys()))]
    
    print("큐에 항목 푸시:")
    for push in proc_push:
        push.start()
    for push in proc_push:
        push.join()
    
    print("대기열에서 항목 팝:")
    for pull in proc_pull:
        pull.start()
    for pull in proc_pull:    
        pull.join()
    
    queue.close()
    queue.join_thread()
    '''
    close()
    현재 프로세스가 이 큐에 더는 데이터를 넣지 않을 것을 나타냅니다. 
    버퍼에 저장된 모든 데이터를 파이프로 플러시 하면 배경 스레드가 종료됩니다. 
    큐가 가비지 수집될 때 자동으로 호출됩니다.

    join_thread()
    배경 스레드에 조인합니다. 
    close() 가 호출된 후에만 사용할 수 있습니다. 
    배경 스레드가 종료될 때까지 블록해서 버퍼의 모든 데이터가 파이프로 플러시 되었음을 보증합니다.
    '''