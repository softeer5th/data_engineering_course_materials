import time
from multiprocessing import Process

def print_continent(name='아시아'):
    print(f"대륙의 이름은 : {name}")
    time.sleep(1)

def main():
    processes = []

    process_default = Process(target=print_continent) # 기본값 : 아시아
    processes.append(process_default)

    continents = ['아메리카', '유럽', '아프리카']
    for continent in continents:
        process = Process(target=print_continent, args=(continent,))
        processes.append(process)

    # 모든 프로세스 시작
    for process in processes:
        process.start()

    # 모든 프로세스가 완료될 때까지 대기
    for process in processes:
        process.join()

    print("모든 프로세스 완료")

# 실행
if __name__ == "__main__":
    main()