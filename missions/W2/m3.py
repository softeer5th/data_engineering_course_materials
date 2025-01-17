# multiprocessing with Queue
import multiprocessing as mp

# debug
# from multiprocessing import Process, util
# util.log_to_stderr(level='DEBUG')  # 코드 시작부분에 추가

# push function for process
def push(q, items):
    for no, item in enumerate(items, start=1):
        q.put(item)
        print('item no:', no, item)


if __name__ == '__main__':
    
    items = ['red', 'green', 'blue', 'black']
    q = mp.Queue()

    process = mp.Process(target=push, args=(q, items))
    process.start()
    process.join()

    # pop
    no = 0
    while not q.empty():
        try:
            print('item no:', no, q.get_nowait())
            no += 1
        except:
            break