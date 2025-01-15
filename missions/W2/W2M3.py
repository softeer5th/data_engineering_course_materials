from multiprocessing import Queue

size = 0
q = Queue()

def push_operation(colors) :
    global size
    print("pushing items to queue:")
    for color in colors :
        size += 1
        q.put(color)
        print(f'item no: {color} {size}')

def pop_operation() :
    global size
    print("popping items to queue:")
    while q :
        size -= 1
        print(f'item no: {q.get()} {size}')

def main() :
    colors = ['red', 'green', 'blue', 'black']
    size = 0

    push_operation(colors)
    pop_operation()

if __name__ == '__main__' :
    main()