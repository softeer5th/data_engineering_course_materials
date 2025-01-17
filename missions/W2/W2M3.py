from multiprocessing import Process, Queue

def push_to_queue(q, colors):
    print("pushing items to queue: ")
    for i, color in enumerate(colors):
        q.put((i, color))
        print(f"item no: {i + 1} {color}")


def pop_from_queue(q):
    print("popping items from queue: ")
    while True:
        try:
            i, color = q.get_nowait()  # 큐에서 항목 가져오기, 비어 있으면 예외 발생
            print(f"Item no: {i}, {color}")
        except Exception as e:
            break


if __name__ == "__main__":
    colors = ["red", "green", "blue", "black"]
    q = Queue()

    # 푸시 및 팝 프로세스 생성
    push_process = Process(target=push_to_queue, args=(q, colors))
    pop_process = Process(target=pop_from_queue, args=(q,))

    # 푸시 프로세스 시작 및 완료 대기
    push_process.start()
    push_process.join()

    # 팝 프로세스 시작 및 완료 대기
    pop_process.start()
    pop_process.join()

    # 큐닫기, 큐 관련 작업이 끝날 때까지 대기
    q.close() 
    q.join_thread()