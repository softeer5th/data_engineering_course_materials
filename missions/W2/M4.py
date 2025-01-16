from multiprocessing import Queue, Process, Event
import time


def provide_task(in_queue: Queue, count: int):
    for i in range(count):
        in_queue.put(f"Task no {i}")
        print(f"Task no {i}")


def consume_task(in_queue: Queue, out_queue: Queue, idx: int, stop_event):
    while not in_queue.empty() or not stop_event.is_set():
        try:
            task = in_queue.get_nowait()
            time.sleep(0.5)
            out_queue.put(f"{task} is done by Process-{idx}")
        except:
            continue


def main():
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    stop_event = Event()

    providers = [
        Process(target=provide_task, args=(tasks_to_accomplish, 100)) for i in range(2)
    ]
    [p.start() for p in providers]

    consumers = [
        Process(
            target=consume_task,
            args=(tasks_to_accomplish, tasks_that_are_done, i, stop_event),
        )
        for i in range(4)
    ]
    [p.start() for p in consumers]

    [p.join() for p in providers]
    # end signal
    stop_event.set()

    [p.join() for p in consumers]

    while not tasks_that_are_done.empty():
        try:
            print(tasks_that_are_done.get_nowait())
        except:
            break


if __name__ == "__main__":
    main()
