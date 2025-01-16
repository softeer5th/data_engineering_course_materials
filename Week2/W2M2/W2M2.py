from multiprocessing import Process

def print_continent(name="아시아"):
    print(f"대륙의 이름은 : {name}")

if __name__ == "__main__":
    default_process = Process(target=print_continent)
    print("Starting default process...")
    default_process.start()

    continents = ["아메리카", "유럽", "아프리카"]
    processes = [Process(target=print_continent, args=(continent,)) for continent in continents]

    for process in processes:
        print(f"Starting process for {process._args[0]}...")
        process.start()

    print("Waiting for default process to complete...")
    default_process.join()
    print("Default process completed.")

    for process, continent in zip(processes, continents):
        print(f"Waiting for process for {continent} to complete...")
        process.join()
        print(f"Process for {continent} completed.")
        
        
# def print_continent(name="아시아"):
#     print(f"대륙의 이름은 : {name}")

# if __name__ == "__main__":
#     default_process = Process(target=print_continent)
#     default_process.start()

#     continents = ["아메리카", "유럽", "아프리카"]
#     processes = [Process(target=print_continent, args=(continent,)) for continent in continents]

#     for process in processes:
#         process.start()

#     # 모든 프로세스가 완료되기를 기다림
#     default_process.join()
#     for process in processes:
#         process.join()
        
        
        
# 동작 순서
# 	1.	default_process.start()로 기본 프로세스를 시작
# 	2.	processes 리스트의 각 프로세스를 시작
# 	3.	default_process.join()로 기본 프로세스가 종료될 때까지 대기
# 	4.	반복문에서 각 프로세스의 join()을 호출하여, 모든 프로세스가 종료될 때까지 대기
# 	5.	모든 프로세스가 종료된 이후에야 스크립트가 종료