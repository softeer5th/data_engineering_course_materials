from multiprocessing import Process
from threading import Thread
from typing import Type, override

from app.core.interfaces import BaseExecutor, BasePool, BaseQueue, T, U


class ThreadPool(BasePool):
    def __init__(
        self,
        worker_count: int,
        timeout: float | None,
        executor_cls: Type[BaseExecutor[T, U]],
        input_queue: BaseQueue[T],
        output_queue: BaseQueue[U],
    ):
        super().__init__(
            Thread,
            worker_count,
            timeout,
            executor_cls,
            input_queue,
            output_queue,
        )


class ProcessPool(BasePool):
    def __init__(
        self,
        worker_count: int,
        timeout: float | None,
        executor_cls: Type[BaseExecutor[T, U]],
        input_queue: BaseQueue[T],
        output_queue: BaseQueue[U],
    ):
        super().__init__(
            Process,
            worker_count,
            timeout,
            executor_cls,
            input_queue,
            output_queue,
        )
