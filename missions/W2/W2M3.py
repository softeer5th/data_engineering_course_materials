from multiprocessing import Process
import time
from multiprocessing import Queue


def push(pid, q, item):
    """아이템을 큐에 추가하는 함수."""
    q.put(item)
    print(f'item No: {pid} {item}')
     

def pop(q):
    i = 0
    while(q):
        print(f'item No: {i} {q.get()}')
        i+=1


if __name__ == '__main__':
    items = ['red','blue','green','black']

    q = Queue()
    print('pushing items to queue:')
    # 각 프로세스가 독립적으로 실행되며, 서로 다른 작업을 수행
    num = len(items)
    for i in range(1,num+1):
        push(i,q,items[i-1])

    print('popping items from queue:')
    pop(q)
