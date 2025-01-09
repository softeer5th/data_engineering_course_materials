import subprocess
from utils import log_separator, log_message

def run_etl():
    try:
        log_separator()
        log_message("ETL 프로세스 시작")

        subprocess.run(['python', 'extract.py'], check=True)
        subprocess.run(['python', 'transform.py'], check=True)
        subprocess.run(['python', 'load.py'], check=True)

        # 시각화 단계 추가
        subprocess.run(['python', 'visualize.py'], check=True)

        log_message("ETL 프로세스 완료")

    except subprocess.CalledProcessError as e:
        log_message(f"ETL 프로세스 실패: {str(e)}")

if __name__ == '__main__':
    run_etl()