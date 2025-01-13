from multiprocessing import Process
import time
    
def work(continent:str='Asia'):
    print(f'The name of continent is : {continent}')
    return

if __name__ == "__main__":
    p1 = Process(target=work, args=['America'])
    p2 = Process(target=work, args=['Europe'])
    p3 = Process(target=work)
    p4 = Process(target=work, args=['Africa'])

    p1.start()
    p2.start()
    p3.start()
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()