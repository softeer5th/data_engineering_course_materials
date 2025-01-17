from multiprocessing import Queue

def push(pid, q, item):
    """아이템을 큐에 추가하는 함수."""
    q.put(item)
    print(f'item No: {pid} {item}')


def pop(q):
    i = 0
    while not q.empty(): #while(q)로 조건 확인하면, q 객체는 항상 있기 때문에 프로그램이 끝나지 않는다. -> q가 비었는지 안 비었는지로 확인해야 함
        a = q.get()

        print(f'item No: {i} {a}')
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
