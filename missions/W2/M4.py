from multiprocessing import Queue, Process, Event
import time
from queue import Empty, Full

TOTAL_TASK = 100
NUM_PRODUCER = 2
PRODUCE_DELAY = 0.01
NUM_CONSUMER = 4
CONSUME_DELAY = 0.001
NUM_PRINTER = 1


def produce_task(in_queue: Queue, idx: int, count: int):
    for i in range(count):
        try:
            in_queue.put_nowait(f"Task no {idx} {i}")
            time.sleep(PRODUCE_DELAY)
        except Full:
            print(f"ERROR: Queue is full. Task no {i} is not added in Producer-{idx}")
    print(f"INFO: Producer-{idx} is done.")


def consume_task(in_queue: Queue, out_queue: Queue, idx: int, stop_event):
    tmp = []
    count = 0
    while not (in_queue.empty() and stop_event.is_set()):
        try:
            task = in_queue.get_nowait()
            count += 1
            time.sleep(CONSUME_DELAY)

            while True:
                try:
                    out_queue.put_nowait(f"{task} is done by Process-{idx}")
                    break
                except Full:
                    print(
                        f"WARN: Queue is full. Task-{task} is not added in Process-{idx}. Retry has been scheduled."
                    )
                    tmp.append(task)
                    break
        except:
            continue

    for task in tmp:
        print(f"INFO: Retry to add task-{task} in Process-{idx}")
        try:
            out_queue.put_nowait(f"{task} is done by Process-{idx}")
        except Full:
            print(f"ERROR: Queue is full. Task-{task} is not added in Process-{idx}")

    print(f"INFO: Consumer-{idx} is done.")
    print(f"INFO: Consumer-{idx} Fairness: {count}")


def print_task(queue: Queue, idx: int, stop_event):
    while not (queue.empty() and stop_event.is_set()):
        try:
            task = queue.get_nowait()
            print(f"PRINT({idx}): {task}")
        except Empty:
            continue
    print(f"INFO: Printer-{idx} is done.")


# 1. Producer가 여러개인 경우
# 2. Producer가 다 끝나기 전부터 Consumer를 시작하고 싶다.
def main():
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    producer_end_event = Event()
    consumer_end_event = Event()

    producer = [
        Process(
            target=produce_task,
            args=(tasks_to_accomplish, i, int(TOTAL_TASK / NUM_PRODUCER)),
        )
        for i in range(NUM_PRODUCER)
    ]
    consumers = [
        Process(
            target=consume_task,
            args=(tasks_to_accomplish, tasks_that_are_done, i, producer_end_event),
        )
        for i in range(NUM_CONSUMER)
    ]
    printer = [
        Process(target=print_task, args=(tasks_that_are_done, i, consumer_end_event))
        for i in range(NUM_PRINTER)
    ]

    for p in printer:
        p.start()

    for p in consumers:
        p.start()

    for p in producer:
        p.start()

    for p in producer:
        p.join()
    producer_end_event.set()

    for p in consumers:
        p.join()
    consumer_end_event.set()

    for p in printer:
        p.join()


if __name__ == "__main__":
    main()
