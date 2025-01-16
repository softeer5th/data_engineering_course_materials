from multiprocessing import Pool
import time

# constants
THREAD_COUNT = 2
THREAD_CONTROL_TUPLE = [('A', 5), ('B', 2), ('C', 1), ('D', 3)]

# thread working function
def workThread(arg:tuple):
    print(f"Process {arg[0]} waiting {arg[1]}seconds")
    time.sleep(arg[1])
    print(f"Proess {arg[0]} Finished.")

# main
if __name__ == "__main__":
    # make threads work
    with Pool(processes=THREAD_COUNT) as p:
        p.map(workThread, THREAD_CONTROL_TUPLE)