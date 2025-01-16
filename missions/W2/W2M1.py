#동시에 작업을 실행하기 위해 워커 풀을 활용하는 스크립트를 작성하여 
#Python의 multiprocessing.Pool에 대한 이해를 보여주세요.

from multiprocessing import Pool
import time

def work_log(job):
    name, work = job
    print(f"Process {name} waiting {work} seconds") #대기 메시지 출력
    time.sleep(work) #지정된 시간 동안 대기
    print(f"Process {name} Finished.") #작업 완료
    return 


if __name__ == "__main__":
    jobs = [('A', 5), ('B', 2), ('C', 1), ('D', 3)] #작업 리스트 정의
    with Pool(processes=2) as pool:
        pool.map(work_log, jobs)
    pool.close()
    pool.join() #모든 프로세스 종료 시까지 대기


