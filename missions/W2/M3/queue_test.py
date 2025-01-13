import multiprocessing as mp
from typing import Any, Callable


def push(queue: mp.Queue, item: Any, size: int):
    """
    Push an item to the queue
    :param queue: queue
    :param item: item to be pushed
    :param size: size of the item
    """
    queue.put(item)
    print(f"item no: {size} {item}")


def pop(queue: mp.Queue, size: int) -> Any:
    """
    Pop an item from the queue
    :param queue: queue
    :param size: size of the item
    :return: popped item
    """
    item = queue.get()
    print(f"item no: {size} {item}")
    return item


if __name__ == "__main__":

    items = ["red", "green", "blue", "black"]

    queue = mp.Queue()

    print("pushing items to queue:")

    counter = 0

    while items:
        counter += 1
        push(queue, items.pop(0), counter)

    print("popping items from queue:")

    counter = 0

    while not queue.empty():
        pop(queue, counter)
        counter += 1
