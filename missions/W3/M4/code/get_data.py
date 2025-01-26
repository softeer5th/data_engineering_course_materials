import kagglehub

# URL 및 파일 경로 설정
data_path = kagglehub.dataset_download("kazanova/sentiment140")
path_file = "/code/path.txt"
with open(path_file, "w") as file:
    file.write(data_path)