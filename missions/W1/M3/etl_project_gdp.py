# 학습 목표
##### 웹사이트에서 데이터를 가져와서 요구사항에 맞게 가공하는 ETL 파이프라인을 만듭니다.
##### - Web Scraping에 대한 이해
##### - Pandas DataFrame에 대한 이해
##### - ETL Process에 대한 이해
##### - Database & SQL 기초

# 사전지식
##### 시나리오
##### 당신은 해외로 사업을 확장하고자 하는 기업에서 Data Engineer로 일하고 있습니다. 경영진에서 GDP가 높은 국가들을 대상으로 사업성을 평가하려고 합니다.
##### 이 자료는 앞으로 경영진에서 지속적으로 요구할 것으로 생각되기 때문에 자동화된 스크립트를 만들어야 합니다.


# 라이브러리 사용
##### web scaping은 BeautifulSoup4 라이브러리를 사용하세요.
##### 데이터 처리를 위해서 pandas 라이브러리를 사용하세요.
##### 로그 기록 시에 datetime 라이브러리를 사용하세요.
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import requests
import re

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

#----------------------------------------------------------------
# Extract
### 국가별 GDP 추출 함수
def get_gdp_by_country():
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

    response = requests.get(url) #응답 성공 시 html 텍스트(문자열 형태) 가져롬
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser') #파싱된 soup객체로 변환
        print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "웹에서 HTTP응답 받음", file=log_file)
    else:
        print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "웹에서 HTTP응답 받지 못함. 응답 코드: {0}".format(response.status_code), file=log_file)
        print(response.status_code)
        return None

    table = soup.select_one('table.wikitable') #선택자로 table찾기
    rows = table.find_all('tr')

    # DataFrame 생성 시 바로 DataFrame에 리스트 컴프리헨션 적용
    df = pd.DataFrame(
        [[col.text.strip() for col in row.find_all(['th', 'td'])][:3] for row in rows],
        columns=['Country', 'GDP_USD_billion', 'year']
    )

    # 첫 번째 헤더 행 및 불필요한 행 제거
    df = df[3:].reset_index(drop=True)
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Extract) raw GDP 데이터 추출", file=log_file)
    return df

### 국가별 Region 추출 함수
def get_region_by_country(df):
    # Country와 Region이 mapping된 csv파일 
    # 출처 - https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
    # 한계점 - 다른 이름 표기법을 사용하거나 데이터 부족으로 맵핑되지 않은 나라가 있을 수 있다.
    region_data = pd.read_csv("missions/W1/M3/region.csv") 
    region_data = region_data.rename(columns={'name':'Country'})
    df = pd.merge(df, region_data[['Country', 'region']], how='left', on='Country') #국가별 GDP 데이터프레임에 나라에 맞는 region맵핑

    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Extract) raw region 데이터 추출", file=log_file)
    return df

### 추출한 raw데이터 json파일로 저장
def save_rawfile():
    json_file_path = "missions/W1/M3/Countries_by_GDP.json"
    df.to_json(json_file_path, orient='records', force_ascii=False)

    print(f"Data has been saved to {json_file_path}")
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), f"raw 데이터 로드: {json_file_path}", file=log_file)

#----------------------------------------------------------------
# Transform

# '-' 처리 함수
def remove_bar(df):
    mask = df['GDP_USD_billion'] == '—' 
    df.loc[mask, ['GDP_USD_billion', 'year']] = None  #GDP가 '-'라면 GDP와 year은 None으로 처리
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Transform) 데이터 결측치 처리", file=log_file)
    return df

#위키피디아 주석 제거 함수
def remove_wiki_annotations(df):
    df['year'] = df['year'].str.replace(r'\[.*?\]', '', regex=True).str.strip()  # 'year' 열에만 적용
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Transform) 불필요한 주석 제거", file=log_file)
    return df

#단위 변경 함수(million -> billion)
def million_to_billion(df):
    df['GDP_USD_billion'] = (
        df['GDP_USD_billion'].str.replace(',', '', regex=False)  # 쉼표 제거
        .astype(float, errors='ignore')  # 문자열을 float로 변환
        .div(1000).round(2)  # billion 단위로 변환, 소수점 2자리까지
    )
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Transform) 데이터 단위 변경", file=log_file)
    return df

#---------------------------------------------------------------
#메인
if __name__ == '__main__':
    log_file = create_logfile()
    df = get_gdp_by_country()
    df = get_region_by_country(df) #E
    save_rawfile()
    df = remove_bar(df)
    df = remove_wiki_annotations(df)
    df = million_to_billion(df) #T

    log_file.close()

#----------------------------------------------------------------
# 화면 출력
##### GDP가 100B USD이상이 되는 국가만을 구해서 화면에 출력해야 합니다.
print(df[df['GDP_USD_billion'] >= 100])

##### 각 Region별로 top5 국가의 GDP 평균을 구해서 화면에 출력해야 합니다.
region_top5 = df.sort_values(by=['GDP_USD_billion'], ascending=False).groupby('region').head(5)
print(region_top5.groupby('region')['GDP_USD_billion'].mean())

#----------------------------------------------------------------
# 팀 활동 요구사항
##### wikipidea 페이지가 아닌, IMF 홈페이지에서 직접 데이터를 가져오는 방법은 없을까요? 어떻게 하면 될까요?
##### 만약 데이터가 갱신되면 과거의 데이터는 어떻게 되어야 할까요? 과거의 데이터를 조회하는 게 필요하다면 ETL 프로세스를 어떻게 변경해야 할까요?

#1. 윤석님이 보내주신 것처럼 IMF 홈페이지 자체에서 공식적으로 제공하는 api와 json파일을 이용하면 된다
#2. 데이터베이스도 flyway와 같은 형상 관리 툴이 있다고 한다. 이런것을 이용하면 되지 않을까했는데, ETL 프로세스 자체를 변경해서 과거 데이터를 조회하려면... 새로운 데이터 로드 시에 기존 데이터를 덮어쓰지 않고 저장해둬야되겠다. 지금 보려고 하는 데이터는 GDP니까 GDP 데이터의 예측 시점을 기준으로 테이블에 새로운 데이터를 붙여나가야 할것 같다.
# 찾아보니 시계열 데이터를 위한 시계열 데이터베이스 같은 경우는 아키텍쳐 디자인이 다르다고 한다. time-stamp기반으로 데이터를 압축, 요약하여 저장한다. 시간 기준으로 아예 파티션을 나눠둔다. 
# 팀원의 아이디어 - GDP같은 경우는 나라 별로 연도에 따라 큰 변동이 없을 것임, 큰 변동이 나타난 나라에 대한 정보만 저장하면 저장할 데이터량이 줄어들 것이다. 