from bs4 import BeautifulSoup
import pandas as pd
import datetime
import requests

### 국가별 GDP 추출 함수
def get_gdp_by_country(log_file):
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
        columns=['Country', 'GDP_USD_million', 'year']
    )

    # 첫 번째 헤더 행 및 불필요한 행 제거
    df = df[3:].reset_index(drop=True)
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Extract) raw GDP 데이터 추출", file=log_file)
    return df

### 국가별 Region 추출 함수
def get_region_by_country(df, log_file):
    # Country와 Region이 mapping된 csv파일 
    # 출처 - https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
    # 한계점 - 다른 이름 표기법을 사용하거나 데이터 부족으로 맵핑되지 않은 나라가 있을 수 있다.
    region_data = pd.read_csv("missions/W1/M3/region.csv") 
    region_data = region_data.rename(columns={'name':'Country'})
    df = pd.merge(df, region_data[['Country', 'region']], how='left', on='Country') #국가별 GDP 데이터프레임에 나라에 맞는 region맵핑

    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Extract) raw region 데이터 추출", file=log_file)
    return df

### 추출한 raw데이터 json파일로 저장
def save_rawfile(df, log_file):
    json_file_path = "missions/W1/M3/Countries_by_GDP.json"
    df.to_json(json_file_path, orient='records', force_ascii=False)

    print(f"Data has been saved to {json_file_path}")
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), f"raw 데이터 로드: {json_file_path}", file=log_file)