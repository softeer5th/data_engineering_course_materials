from abc import ABC, abstractmethod
from multiprocessing import Event
from typing import Any, Generic, Optional, Type, TypeVar

from app.core.exceptions import QueueEmptyException, QueueFullException

T = TypeVar("T")
U = TypeVar("U")

Item = TypeVar("Item", bound=Optional[Any])
Counter = TypeVar("Counter")
Lock = TypeVar("Lock")
Queue = TypeVar("Queue")
Task = TypeVar("Task")


class BaseQueue(Generic[Item]):
    def __init__(
        self,
        counter: Type[Counter],
        lock: Type[Lock],
        queue: Type[Queue],
        maxsize: int = 0,
    ):
        self._size = counter
        self._max_size = maxsize
        self._external_lock = lock()
        self._io_lock = lock()
        self._queue = queue(maxsize=maxsize)

    @property
    def size(self) -> int:
        return self._size.value

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def lock(self) -> Lock:
        return self._external_lock

    def put(self, item: Item, timeout: float | None = None) -> None:
        with self._io_lock:
            self._queue.put(item, timeout=timeout)
            self._size.value += 1

    def get(self, timeout: float | None = None) -> Item:
        with self._io_lock:
            res = self._queue.get(timeout=timeout)
            self._size.value -= 1
        return res

    def put_nowait(self, item: Item) -> None:
        with self._io_lock:
            if self._size.value == self._max_size:
                raise QueueFullException
            self._queue.put(item)
            self._size.value += 1

    def get_nowait(self) -> Item:
        with self._io_lock:
            if self._size.value == 0:
                raise QueueEmptyException
            res = self._queue.get()
            self._size.value -= 1
            return res

    def empty(self) -> bool:
        return self._size.value == 0

    def full(self) -> bool:
        return self._size.value == self._max_size


class BaseExecutor(ABC, Generic[T, U]):
    @abstractmethod
    def execute(self, item: T) -> U: ...


class BasePool(ABC):
    def __init__(
        self,
        task_cls: Type[Task],
        worker_count: int,
        timeout: float | None,
        executor_cls: Type[BaseExecutor[T, U]],
        input_queue: BaseQueue[T],
        output_queue: BaseQueue[U],
    ):
        self.task_cls = task_cls
        self.worker_count = worker_count
        self.timeout = timeout
        self.executor_cls = executor_cls
        self.input_queue = input_queue
        self.output_queue = output_queue

    @staticmethod
    def _worker(
        stop_event,
        timeout: float | None,
        executor_cls: Type[BaseExecutor[T, U]],
        input_queue: BaseQueue[T],
        output_queue: BaseQueue[U],
    ) -> None:
        executor = executor_cls()
        while not stop_event.is_set():
            with input_queue.lock:
                item = input_queue.get(timeout=timeout)
                if item is None:
                    stop_event.set()
                    input_queue.put(None)
                    break
            res = executor.execute(item)
            output_queue.put(res)
            print(f"Output queue size: {output_queue.size}")

    def execute(self) -> None:
        tasks: list[Task] = []
        stop_event = Event()
        for i in range(self.worker_count):
            task = self.task_cls(
                target=BasePool._worker,
                args=(
                    stop_event,
                    self.timeout,
                    self.executor_cls,
                    self.input_queue,
                    self.output_queue,
                ),
            )
            task.start()
            tasks.append(task)

        for task in tasks:
            task.join()

        self.output_queue.put(None)


class BaseContext(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs): ...

    @abstractmethod
    def __enter__(self) -> BasePool: ...

    @abstractmethod
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool: ...
