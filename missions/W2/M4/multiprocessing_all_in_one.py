from multiprocessing import Process, Queue, Lock, current_process
import queue
import time
    
def pull_task(pull_q:Queue, push_q:Queue, lock):
    while True:
        
        try:
            with lock:
                num = pull_q.get_nowait()
        
        except queue.Empty as e:
            # print(f"Queue is empty: {current_process().name}", e)
            break
        
        else:
            time.sleep(0.5)
            push_q.put(msg := f"Task no {num} is done by {current_process().name}")
            # print(f"Push {num} by {current_process().name}")
    
    
if __name__ == "__main__":
    
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    
    lock = Lock()
    
    for i in range(10):
        print(f"Task no {i}")
        tasks_to_accomplish.put(i)
        
               
    procs = [Process(target=pull_task, args=(tasks_to_accomplish, tasks_that_are_done, lock))for _ in range(4)]
    
    for proc in procs:
        proc.start()
        
    for proc in procs:
        proc.join()
        
    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())
    
    tasks_to_accomplish.close()
    tasks_to_accomplish.join_thread()
    
    tasks_that_are_done.close()
    tasks_that_are_done.join_thread()
    