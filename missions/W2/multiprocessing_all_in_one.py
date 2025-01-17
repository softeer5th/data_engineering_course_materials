from multiprocessing import Process
from multiprocessing import Queue
import time

     
def work(q1,q2,i):
    while True: ##멀티 프로세스로 실행하고 있어서 empty를 확인한 시점과 작업하는 시점이 다르면 안된다. -> while not q1.empty(): X while True: O 
        try:
            a = q1.get_nowait() ##get_nowait()는 아무것도 없어도 기다려주지 않는다 ->try except 필수임.
        except Exception as e:
            #print(f"Process-{i}: Error encountered: {e}")
            break
        else:
            print(f'item No: {a}')
            time.sleep(0.5)
            message = f'Task No {a} is done by Process-{i}'
            q2.put(message)
            #print(message)


if __name__ == '__main__':
    task_list = [0,1,2,3,4,5,6,7,8,9]

    #작업할 큐
    task_to_complish = Queue()

    #작업할 큐에 task 추가
    for i in range(len(task_list)):
        task_to_complish.put(task_list[i])

    #결과 큐 생성
    tasks_that_are_done= Queue()

    # 각 프로세스가 독립적으로 실행되며, 서로 다른 작업을 수행
    processes = [Process(target=work, args=(task_to_complish,tasks_that_are_done, i)) for i in range(4)]
    for process in processes:
        process.start()

    for process in processes:
        process.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())
