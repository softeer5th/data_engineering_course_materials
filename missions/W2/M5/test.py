from konlpy.tag import Kkma,Okt
text = "수학 손도 대기 싫으신 분들을 위한 강의 !!"
# text = "이 영화는 스토리도 연출도 좋아요. 그러나 조금 지루합니다."
# r = Kkma().pos(text)
# print(r)
# nltk.download('stopwords')s
stopwords = [
    '이', '나오', '있', '가지', '하', '씨', '것', '시키', '들', '만들', 
    '그', '지금', '되', '생각하', '수', '그러', '이', '속', '보', '하나', 
    '않', '집', '없', '살', '나', '모르', '사람', '적', '주', '월', '아니', 
    '데', '등', '자신', '같', '안', '우리', '어떤', '때', '내', '년', '내', 
    '가', '경우', '한', '명', '지', '생각', '대하', '시간', '오', '그녀', 
    '말', '다시', '일', '이런', '그렇', '앞', '위하', '보이', '때문', '번', 
    '그것', '나', '두', '다른', '말하', '어떻', '알', '여자', '그러나', '개', 
    '받', '전', '못하', '들', '일', '사실', '그런', '이렇', '또', '점', 
    '문제', '싶', '더', '말', '사회', '정도', '많', '좀', '그리고', '원', 
    '좋', '잘', '크', '통하', '따르', '소리', '중', '놓'
    ]
types = ['Adjective', 'Adverb', 'Exclamation', 'Noun', 'Number', 'Verb']
# r2 = list(map(Okt().pos, Kkma().morphs(text)))
# print(r2)
# r2 = Okt().morphs(text)
# r3 = list(map(Okt().pos, r2))
r3 = Okt().pos(text)
# print(r3)
# print(Okt().tagset)
r4 = [w[0] for w in r3 if (w[0] not in stopwords) and (w[1] in types)]
print(r4)