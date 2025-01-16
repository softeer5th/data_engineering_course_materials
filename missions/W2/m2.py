import multiprocessing as mp

def print_continent(continent="Asia"):
    print(f"The name of continent is : {continent}")

if __name__ == '__main__':
    number_of_process = 4
    data = ['America', 'Europe', 'Africa']

    processes = [mp.Process(target=print_continent)]
    for i in range(1, number_of_process):
        processes.append(mp.Process(target=print_continent, args=(data[i-1],)))

    for process in processes:
        process.start()

    for process in processes:
        process.join()
