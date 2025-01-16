from multiprocessing import Process

def continent(con) :
    print(f'The name of continent is : {con}')

def main() :
    continents = [None, 'America', 'Europe', 'Africa']
    processes = []
    for con in continents :
        proc = Process(target = continent, args=(con if con != None else 'Asia',))
        proc.start()
        processes.append(proc)

    for proc in processes :
        proc.join()

if __name__ == '__main__' :
    main()