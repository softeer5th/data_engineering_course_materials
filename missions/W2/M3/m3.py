from multiprocessing import Process, Queue

def pushColor(colors:list, q:Queue):
    for idx, color in enumerate(colors):
        q.put(color)
        print(f'item no: {idx + 1} {color}')
    return

def popColor(q:Queue):
    i = 0
    while not q.empty():
        print(f'item no: {i} {q.get()}')
        i += 1
    return

if __name__ == '__main__':
    colors = ['red', 'green', 'blue', 'black']
    q = Queue()

    print(f'pushing items to queue:')
    p1 = Process(target=pushColor, args=[colors, q])
    p1.start()
    p1.join()

    print(f'popping items from queue:')
    p2 = Process(target=popColor, args=[q])
    p2.start()
    p2.join()