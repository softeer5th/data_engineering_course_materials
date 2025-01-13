from multiprocessing import Queue

if __name__ == '__main__':
    colors = ['red', 'green', 'blue', 'black']

    q = Queue()
    print('pushing items to queue:')
    for id, color in enumerate(colors):
        q.put((id, color))
        print(f'item no: {id + 1} {color}')

    print('popping items to queue:')
    while q:
        id, color = q.get()
        print(f'item no: {id} {color}')
    
    q.close()
    q.join_thread()