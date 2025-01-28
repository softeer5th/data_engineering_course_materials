from multiprocessing import Queue, Process
from multiprocessing import current_process
import time
from queue import Empty

def processing(tasks_to_accomplish, tasks_that_are_done ) :
    while True:
        try :
            num = tasks_to_accomplish.get_nowait()
        except Empty:
            # get_nowait() 함수로 인해 큐가 비어있다는 예외가 발생한다면(더 진행할 테스크가 없다면) 중단.
            break
        except Exception as e :
            # 큐가 비어있는 상황 이외에 예외가 get_nowait에서 발생하였을 때 처리.
            print(f"Unexpected Exception of get_nowait() : {e}")
        
        try :
            # 테스크가 시작될 때 테스크 번호 출력
            print(f"Task no {num}")
            time.sleep(0.5)
            tasks_that_are_done.put(num)
            # 테스크가 끝났을 때 테스크 번호와 프로세스 번호 출력
            print(f"Task no {num} is done by {current_process().name}")
        except Exception as e :
            # 예상치 못한 예외가 발생되었을 때 처리.
            print(f"Unexpected Exception : {e}")

def main() :

    tasks = [i for i in range(10)]
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    processes = []

    # 진행해야하는 테스크를 큐를 이용해서 관리
    for task in tasks :
        tasks_to_accomplish.put(task)

    # 프로세스 4개 생성
    for i in range(1,5) :
        proc = Process(name = f'Process {i}', target = processing, args = (tasks_to_accomplish, tasks_that_are_done))
        proc.start()
        processes.append(proc)

    # 프로세스 마무리 및 리소스 정리
    for proc in processes :
        proc.join()

if __name__ == "__main__" :
    main()
