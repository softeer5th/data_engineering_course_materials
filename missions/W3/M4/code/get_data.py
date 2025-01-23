import kagglehub
# import os
from pathlib import Path

# URL 및 파일 경로 설정
data_name = '/training.1600000.processed.noemoticon.csv'
# file_full_path = os.path.abspath(__file__)
# path = os.path.dirname(file_full_path)
# print(path)
path = Path(__file__).parent
print(path)
# data_path = kagglehub.dataset_download("kazanova/sentiment140", '/Users/sumin/workspace/softeer_de_repo/missions/W3/M4/code/')
data_path = kagglehub.dataset_download("kazanova/sentiment140")
print(data_path)
# def download_file(url):


# def main():
#     if not os.path.exists(data_name):
        
#     else:
#         print("data file already exists.")

# if __name__ == "__main__":
#     main()
