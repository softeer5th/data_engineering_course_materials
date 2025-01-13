import multiprocessing as mp
import time
from typing import Any, Callable


def execute_task(name: str, duration: int):
    """
    Execute a task
    :param name: name of the task
    :param duration: duration of the task
    """
    print(f"Process {name} waiting {duration} seconds")
    time.sleep(duration)
    print(f"Process {name} finished.")


def run_parallel(
    args_list: list[tuple], function: Callable, process_count: int
):
    """
    Run functions in parallel
    :param args_list: list of arguments
    :param function: function to be executed
    :param process_count: number of processes
    """
    with mp.Pool(processes=process_count) as pool:
        pool.starmap(function, args_list)


if __name__ == "__main__":
    tasks = [
        ("A", 5),
        ("B", 2),
        ("C", 1),
        ("D", 3),
    ]
    run_parallel(tasks, execute_task, 2)
