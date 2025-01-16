from multiprocessing import Queue


def push_operation(queue: Queue, items: list[str]):
    for index, item in enumerate(items):
        print(f"item no: {index+1} {item}")
        queue.put(item)


def pop_operation(queue: Queue):
    idx = 0
    while True:
        item = queue.get()
        if item is None:
            break
        print(f"item no: {idx} {item}")
        idx += 1


def main():
    items = ["red", "green", "blue", "black"]
    queue = Queue()

    print("pushing items to queue:")
    push_operation(queue, items)
    # None: Marker for termination
    # Queue의 Consumer가 하나라면 가능하지만 여러개라면 불가능한 접근.
    queue.put(None)

    print("popping items from queue:")
    pop_operation(queue)


if __name__ == "__main__":
    main()
