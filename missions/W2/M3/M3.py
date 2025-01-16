import multiprocessing as mp

# constants
COLOR = ['red', 'green', 'blue', 'black']

# working function
def work(q:mp.Queue, iQ:mp.Queue, color:str):
    i = iQ.get()    # get semaphore

    # processing: put color to the Queue
    q.put(color)
    print(f"item no: {i} {color}")
    
    iQ.put(i+1)     # return semaphore

if __name__ == "__main__":
    proc = []           # processes
    queue = mp.Queue()  # Color Queue
    numQ = mp.Queue()   # number Queue; used like a semaphore

    numQ.put(1) # initialize number Queue
    
    # processes start
    print("pushing items to queue:")

    for i in range(len(COLOR)):
        p = mp.Process(target=work, args=(queue, numQ, COLOR[i]))
        p.start()
        proc.append(p)

    # wait for processes
    for p in proc:
        p.join()
    
    # print result
    print("popping items form queue:")

    i = 0
    while not queue.empty():
        print(f"item no: {i} {queue.get()}")
        i += 1