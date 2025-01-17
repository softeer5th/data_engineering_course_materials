import datetime

# ETL 프로세스 기록용 로그파일 생성
def create_logfile():
    # 시간 기록 - 'Year-Monthname-Day-Hour-Minute-Second'
    try:
        log_file = open('missions/W1/M3/etl_project_log.txt', 'x') #파일 없으면 생성, 있으면 에러 발생
        print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "로그 파일 생성", file=log_file)
        return log_file
    except:
        log_file = open('missions/W1/M3/etl_project_log.txt', 'a') #파일 있으면 이어쓰기
        return log_file