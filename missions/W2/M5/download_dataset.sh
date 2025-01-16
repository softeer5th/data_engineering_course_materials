#!/bin/bash

# 데이터 경로
DATA_DIR="temp"

# dataset 디렉토리가 없으면 생성
if [ ! -d "$DATA_DIR" ]; then
   mkdir -p "$DATA_DIR"
fi

# curl로 다운로드 (-L: 리다이렉트 따라가기, -o: 출력 파일 지정)
curl -L -o "$DATA_DIR/sentiment140.zip" \
   https://www.kaggle.com/api/v1/datasets/download/kazanova/sentiment140

if [ $? -eq 0 ]; then
   # dataset 디렉토리에서 zip 파일 압축 해제
   unzip -o "$DATA_DIR/sentiment140.zip" -d "$DATA_DIR/"
   
   # 압축 해제 성공 여부 확인
   if [ $? -eq 0 ]; then
       # 압축 해제된 파일 찾기 및 이름 변경
       extracted_file=$(find "$DATA_DIR" -type f -not -name "sentiment140.zip" -not -name "sentiment140")
       if [ -n "$extracted_file" ]; then
           mv "$extracted_file" "$DATA_DIR/sentiment140.csv"
           echo "다운로드, 압축 해제 및 파일 이름 변경 완료"
           # zip 파일 삭제
           rm "$DATA_DIR/sentiment140.zip"
       else
           echo "압축 해제된 파일을 찾을 수 없습니다"
           exit 1
       fi
   else
       echo "압축 해제 실패"
       exit 1
   fi
else
   echo "다운로드 실패"
   exit 1
fi