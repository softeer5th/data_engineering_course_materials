import multiprocessing as mp
import time

def work_log(task: tuple):
    task_name, duration = task
    print(f"Process {task_name} waiting for {duration} seconds")
    time.sleep(duration)
    print(f"Process {task_name} Finished.") 
    
def m1(pool_size=2):
    tasks = [
        ('A', 5),
        ('B', 2),
        ('C', 1),
        ('D', 3),
    ]
    # map 미사용
    # with mp.Pool(pool_size) as pool:
    #     results = []
    #     for t in tasks:
    #         res = pool.apply_async(work_log, args=t)
    #         results.append(res)
        
    #     for res in results:            
    #         res.get()

    # map 사용
    with mp.Pool(pool_size) as pool:
        pool.map(work_log, tasks) # 튜플로 받아서 함수에 넘겨줌.

def print_continent(continent: str = None):
    if continent == "America":
        print(f"The name of continent is {continent}")
    elif continent == "Europe":
        print(f"The name of continent is {continent}")
    elif continent == "Africa":
        print(f"The name of continent is {continent}")
    else:
        print(f"The name of continent is Asia")
    

def m2():
    processes = []
    for continent in [None, "America", "Europe", "Africa"]:
        p = mp.Process(target=print_continent, args=(continent,))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()

def m3():
    q = mp.Queue()

    # Push operation
    print("pushing items to queue:")
    for i, color in enumerate(['red', 'green', 'blue', 'black']):
        q.put(color)
        print(f"item no: {i+1} {color}")
        # qsize는 현재 큐에 들어있는 아이템의 개수를 반환함.
        # Unix 계열인 macOS에서는 qsize가 NotImplementedError가 나옴.
        # Note that this may raise NotImplementedError on Unix platforms like macOS where sem_getvalue() is not implemented.
        # qsize, empty, full 등을 
        # 다중 스레딩/ 다중 프로세스 환경에서 신뢰할 수 없음.
    
    # None을 넣어서 큐에 더 이상 들어오는 아이템이 없다는 것을 알림. 
    # 단일 프로세스여서 가능한 일.
    q.put(None) 

    # Pop operation
    print("Popping items from queue:")
    index = 0
    item = q.get()
    print(f"item no: {index} {item}")
    while item is not None: # 다중 스레딩/ 다중 프로세스 환경에서 신뢰할 수 없음.
        index += 1
        print(f"item no: {index} {q.get()}")

    
if __name__ == "__main__":
    mp.set_start_method('spawn') # macOS에서 기본값: spawn
    # fork 시작 방법은 macOS에서는 서브 프로세스 충돌 가능성 존재.
    # 단 한번만 실행해야 함.
    m1(pool_size=2)
    m2()
    m3()


# 아래와 같이 하면 무한 에러. spawn으로 만들 시에 전체 코드를 다시 실행하기에
# if __name__ == "__main__":이 없으면 무한 루프에 빠짐.
"""
from multiprocessing import Pool

def f(x):
    return x*x

p = Pool(5)
with p:
    p.map(f, [1,2,3])
"""

