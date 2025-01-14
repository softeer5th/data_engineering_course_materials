import multiprocessing as mp
import time
from queue import Empty


class Task:
    """
    Task class
    """

    def __init__(self, name: int):
        """
        Task class constructor
        :param name: name of the task
        """
        self.name = name

    def __repr__(self):
        """
        Task class representation
        :return: string representation of the task
        """
        return f"Task no {self.name}"


def process_task(worker_name: int, task: Task) -> str:
    """
    Process a task
    :param worker_name: name of the worker
    :param task: task to be processed
    :return: message
    """
    print(task)
    time.sleep(0.5)
    msg = f"{task} is done by Process-{worker_name}"
    return msg


def worker(name: int, task_queue: mp.Queue, done_queue: mp.Queue):
    """
    Worker function
    :param name: name of the worker
    :param task_queue: queue of tasks
    :param done_queue: queue of done tasks
    """
    while True:
        try:
            task: Task = task_queue.get_nowait()
            msg = process_task(name, task)
            done_queue.put(msg)
        except Empty:
            break


def process_tasks(
    task_queue: mp.Queue, done_queue: mp.Queue, process_count: int
):
    """
    Process tasks
    :param task_queue: queue of tasks
    :param done_queue: queue of done tasks
    :param process_count: number of processes
    """
    processes = [
        mp.Process(target=worker, args=(i + 1, task_queue, done_queue))
        for i in range(process_count)
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


def print_results(done_queue: mp.Queue):
    """
    Print results
    :param done_queue: queue of done tasks
    """
    while not done_queue.empty():
        msg = done_queue.get()
        print(msg)


def main():
    task_count = 10
    process_count = 4

    tasks_to_accomplish = mp.Queue()

    for i in range(task_count):
        tasks_to_accomplish.put(Task(i))

    tasks_that_are_done = mp.Queue()

    process_tasks(tasks_to_accomplish, tasks_that_are_done, process_count)

    print_results(tasks_that_are_done)


if __name__ == "__main__":
    main()
