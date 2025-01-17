from multiprocessing import Queue as MPQueue
from multiprocessing import Value, get_context
from multiprocessing.synchronize import Lock
from queue import Queue
from threading import Lock as ThreadLock

from app.core.interfaces import BaseQueue, Item


def ProcessLock():
    return Lock(ctx=get_context())


class ThreadCounter:
    value: int = 0


class ThreadSafeQueue(BaseQueue[Item]):
    def __init__(self, maxsize: int = 0):
        super().__init__(ThreadCounter(), ThreadLock, Queue, maxsize=maxsize)


class ProcessSafeQueue(BaseQueue[Item]):
    def __init__(self, maxsize: int = 0):
        super().__init__(Value("i", 0), ProcessLock, MPQueue, maxsize=maxsize)
