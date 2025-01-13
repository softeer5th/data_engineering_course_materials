from multiprocessing import Queue

def put(color : str, q : Queue, number : int):
    q.put(color)
    print("item no: {number} {color}".format(number = number, color = color))

def get(q : Queue, number : int):
    data = q.get_nowait()
    print("item no: {number} {color}".format(number = number, color = data))

if __name__ == "__main__":
    q = Queue()
    colors = ["red","green","blue","black"]
    print("pushing items to queue:")
    count = 1
    for color in colors:
        put(color=color, q=q, number = count)
        count += 1
    print("pushing items to queue:")
    
    count = 0
    while True :
        try :
            get(q, count)
            count += 1
        except :
            break

    q.close()