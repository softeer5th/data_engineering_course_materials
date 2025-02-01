from multiprocessing import Process, Queue, current_process
import time

def worker(tasks_to_accomplish, tasks_that_are_done):
    while True:
        try:
            task = tasks_to_accomplish.get_nowait()
        except Exception:
            break
        else:
            print(f"{task} is being executed by {current_process().name}")
            time.sleep(0.5)
            tasks_that_are_done.put(f"{task} is done by {current_process().name}")

def main():
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    
    for task_no in range(10):
        task = f"Task no {task_no}"
        print(task)
        tasks_to_accomplish.put(task)

    processes = []
    for i in range(4):
        process = Process(target=worker, args=(tasks_to_accomplish, tasks_that_are_done))
        processes.append(process)
        process.start()
        
    for process in processes:
        process.join()
        
    print("\nTask Completion Results:")
    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())  
    
if __name__ == "__main__":
    main()