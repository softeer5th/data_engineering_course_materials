import sys

current_word = None #현재 단어
current_count = 0 #현재 단어의 누적 개수
word = None #입력에서 가져온 단어

# Reducer code
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1) #탭을 기준으로 최대 한번만 분리
    try: 
        count = int(count)
    except ValueError:
        continue
    
    #현재 단어와 이전 단어가 같다면 개수 누적
    if current_word == word:
        current_count += count
    #새로운 단어가 등장하면 이전 단어와 개수를 출력
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")  # 이전 단어의 결과 출력
        current_word = word
        current_count = count

# 마지막 단어 출력
if current_word == word:
    print(f"{current_word}\t{current_count}")
