from multiprocessing import Process

def print_name(continent='아시아'): #기본값은 아시아
    print(f"The name of continent is : {continent}")
    

if __name__ == "__main__":
    continents = ["아메리카", "유럽", "아프리카"]
    procs = [Process(target=print_name)] #기본 대륙 프로세스 추가 

    for c in continents: #다른 대륙 프로세스 추가 
        proc = Process(target=print_name, args=(c,))
        procs.append(proc)
    
    #모든 프로세스 시작
    for proc in procs:
        proc.start()

    #모든 프로세스 완료 대기
    for proc in procs:
        proc.join()