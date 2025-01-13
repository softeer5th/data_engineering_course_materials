import multiprocessing as mp
from typing import Any, Callable


def print_continent(name: str = "Asia"):
    """
    Print the name of the continent
    :param name: name of the continent
    """
    print(f"The name of continent is : {name}")


def run_parallel(args_list: list[tuple], function: Callable):
    """
    Run functions in parallel
    :param args_list: list of arguments
    :param function: function to be executed
    """
    processes = [mp.Process(target=function, args=args) for args in args_list]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    continents = [None, "America", "Africa", "Europe"]

    args_list = [
        (continent,) if continent is not None else ()
        for continent in continents
    ]

    run_parallel(args_list, print_continent)
