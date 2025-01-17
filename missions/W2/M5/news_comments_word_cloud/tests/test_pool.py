import os
import time
from typing import Any, override

from app.core.interfaces import BaseContext, BaseExecutor, BasePool
from app.core.pool import ProcessPool, ThreadPool
from app.core.queue import ProcessSafeQueue, ThreadSafeQueue


class Extractor(BaseExecutor[int, int]):
    @override
    def execute(self, item: int) -> int:
        pid = os.getpid()
        time.sleep(item / 20)
        print(f"[PID: {pid}] Excecuted {item}")
        return item * 2


class ExtractorContext(BaseContext):
    @override
    def __init__(self, pool: BasePool):
        self._pool = pool

    @override
    def __enter__(self) -> BasePool:
        print("Entering context")
        return self._pool

    @override
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        print("Exiting context")


def test_thread_pool():
    data = []
    for i in range(10):
        data.append(i + 1)
    data.append(None)

    input_queue = ThreadSafeQueue()
    output_queue = ThreadSafeQueue()

    for item in data:
        input_queue.put(item)

    with ExtractorContext(
        ThreadPool(3, None, Extractor, input_queue, output_queue)
    ) as thread_pool:
        thread_pool.execute()

    print(f"Output queue size: {output_queue.size}")
    while not output_queue.empty():
        print(output_queue.get())


def test_process_pool():
    data = []
    for i in range(10):
        data.append(i + 1)
    data.append(None)

    input_queue = ProcessSafeQueue()
    output_queue = ProcessSafeQueue()

    for item in data:
        input_queue.put(item)

    with ExtractorContext(
        ProcessPool(3, None, Extractor, input_queue, output_queue)
    ) as thread_pool:
        thread_pool.execute()

    print(f"Output queue size: {output_queue.size}")
    while not output_queue.empty():
        print(output_queue.get())
