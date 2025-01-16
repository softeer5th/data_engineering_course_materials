from konlpy.tag import Kkma,Okt
import nltk
text = "수학 손도 대기 싫으신 분들을 위한 강의 !!"
text = "이 영화는 스토리도 연출도 좋아요. 그러나 조금 지루합니다."
r = Kkma().pos(text)
print(r)
nltk.download('stopwords')
nltk.download('punkt_tab')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
# stopwords_kr = stopwords.words('korean')
print(word_tokenize(text, language="korean"))
rr = text.split()
print(rr)
# print(type(r))

# r2 = list(map(Okt().pos, Kkma().morphs(text)))
# print(r2)
# r2 = Okt().morphs(text)
# r3 = list(map(Okt().pos, r2))
r3 = Okt().pos(text)
print(r3)