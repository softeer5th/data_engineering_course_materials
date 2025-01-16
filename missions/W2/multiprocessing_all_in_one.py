from multiprocessing import Process
from multiprocessing import Queue
import time

     
def work(q1,q2,i):
    while not q1.empty(): ##q1에 요소가 하나라도 있으면
        try:
            a = q1.get_nowait() ###get_nowait()를 사용해야 하는 이유는 뭘까? empty가 정확하지 않음. -> 큐에 아무것도 없어도 일단 코드 실행
        except Exception as e:
            print(f"Process-{i}: Error encountered: {e}")
            break
        else:
            time.sleep(0.5)
            message = f'Task No {a} is done by Process-{i}'
            q2.put(message)
            #print(message)

def pop(q):
    i = 0
    while(q):
        print(f'item No: {i} {q.get()}')
        i+=1

if __name__ == '__main__':
    task_list = [0,1,2,3,4,5,6,7,8,9]

    #작업큐
    task_to_complish = Queue()
    # 각 프로세스가 독립적으로 실행되며, 서로 다른 작업을 수행
    for i in range(len(task_list)):
        task_to_complish.put(task_list[i])

    #결과 큐    
    tasks_that_are_done= Queue()

    processes = [Process(target=work, args=(task_to_complish,tasks_that_are_done, i)) for i in range(4)]
    for process in processes:
        process.start()

    for process in processes:
        process.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())


