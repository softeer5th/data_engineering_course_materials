from multiprocessing import Process

def print_continent(continent: str='Asia')-> None:
    print(f'The name of continent is : {continent}', flush=True)


if __name__ == '__main__':
    continents = ['Europe', 'Africa', 'America']

    procs = [Process(target=print_continent)]
    procs[0].start()

    for c in continents:
        p = Process(target=print_continent, args=(c, ))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()
    