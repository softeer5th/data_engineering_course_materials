from multiprocessing import Process, Queue
import time 

# Queue가 균등히 분배 되지 않고, 어느 하나에 치중되는 경우가 있음 -> queue의 empty가 빨리 호출됨
# Queue의 empty가 왜 빨리가게 되는거지.
# get_nowait : lock에 걸린 상태가 empty로 예외로 호출됨.

def run(que_to : Queue, que_done : Queue, processNumber : int):
    while True:
        try :
            # 원자성을 지키기 위해서 while 에 True를 넣고, get_nowait()를 쓰는 것도 좋다.
            task = que_to.get_nowait()
            print(task)
            time.sleep(0.5)
            que_done.put("{task} is done by Process-{number}".format(task = task, number =processNumber))
        except :
            break

if __name__ == "__main__":
    tasks = [
        "Task no 0",
        "Task no 1",
        "Task no 2",
        "Task no 3",
        "Task no 4",
        "Task no 5",
        "Task no 6",
        "Task no 7",
        "Task no 8",
        "Task no 9"
    ]

    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()

    for task in tasks: 
        tasks_to_accomplish.put(task)

    processList = []
    for i in range(4):
        processList.append(Process(target=run, args=(tasks_to_accomplish,tasks_that_are_done, i+1)))

    for process in processList:
        process.start()

    for process in processList:
        process.join()

    while True :
        try :
            message = tasks_that_are_done.get_nowait()
            print(message)
        except :
            break