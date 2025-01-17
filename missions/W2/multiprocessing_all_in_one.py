# multiprocessing queues + processes
import time
import multiprocessing as mp

# function-execute_task for process
def execute_task(tasks_to_accomplish, tasks_that_are_done, id):
    while True:
        try:
            task = tasks_to_accomplish.get_nowait() # raise Except when queue is empty
            print(task)
            time.sleep(0.5)
            task_done = task + ' is done by Process-' + id
            tasks_that_are_done.put(task_done)
        except:
            return 0

# main
if __name__ == "__main__":
    # Task distribution
    tasks_to_accomplish = mp.Queue()
    tasks_that_are_done = mp.Queue()
    for i in range(10):
        task = "Task no " + str(i)
        tasks_to_accomplish.put(task)

    # Process execution
    num_process = 4
    processes = [
        mp.Process(target=execute_task, args=(tasks_to_accomplish, tasks_that_are_done, str(i)))
        for i in range(1, num_process + 1)]

    # Process start
    for process in processes:
        process.start()
   
    # Wait for all processes to terminate
    for process in processes:
        process.join()
    
    while True:
        try:
            print(tasks_that_are_done.get_nowait())
        except:
            break