from multiprocessing import Process, Queue

def push(queue, items):
    print("큐에 항목 푸시:")
    for i, item in enumerate(items):
        print(f"항목 번호: {i+1} {item}")
        queue.put(item)

def pop(queue):
    print("대기열에서 항목 팝:")
    i = 0
    while not queue.empty():
        item = queue.get()
        print(f"항목 번호: {i} {item}")
        i += 1

def main():
    queue = Queue()
    items = ['빨간색', '녹색', '파란색', '검정색']

    push_process = Process(target=push, args=(queue, items))
    pop_process = Process(target=pop, args=(queue,))

    push_process.start()
    push_process.join()
    
    pop_process.start()
    pop_process.join()

    print("모든 프로세스 완료")

# 실행
if __name__ == "__main__":
    main()