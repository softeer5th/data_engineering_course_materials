from multiprocessing import Process
import time

def print_continent(name):
    if name=='':
        name = 'Asia'
    print(f'The name of continent is : {name}')


if __name__ == '__main__':
    names = [ '','America','Europe','Africa']
    #print(names[0])
    num_of_processes = 2

    # 각 프로세스가 독립적으로 실행되며, 서로 다른 작업을 수행
    num_of_processes = len(names)
    processes = []

    for idx,name in enumerate(names):
        process = Process(target=print_continent, args=(name,)) 
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
