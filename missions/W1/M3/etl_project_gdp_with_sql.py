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
import Extract
import Transform
import Load
import Logfile
import sqlite3
#----------------------------------------------------------------
# 추가 요구 사항
### 코드를 수정해서 아래 요구사항을 구현하세요.
##### 추출한 데이터를 데이터베이스에 저장하세요. 'Countries_by_GDP'라는 테이블명으로 'World_Economies.db'라는 데이터 베이스에 저장되어야 합니다. 해당 테이블은 'Country', 'GDP_USD_billion'라는 어트리뷰트를 반드시 가져야 합니다.
##### 데이터베이스는 sqlite3 라이브러리를 사용해서 만드세요.
##### 필요한 모든 작업을 수행하는 'etl_project_gdp_with_sql.py' 코드를 작성하세요.


if __name__ == '__main__':
    log_file = Logfile.create_logfile()
    df = Extract.get_gdp_by_country(log_file)
    df = Extract.get_region_by_country(df, log_file)
    Extract.save_rawfile(df, log_file)
    df = Transform.remove_bar(df, log_file)
    df = Transform.remove_wiki_annotations(df, log_file)
    df = Transform.million_to_billion(df, log_file)
    Load.connect_and_load(df, log_file)
    log_file.close()

#----------------------------------------------------------------
# 화면 출력1
##### GDP가 100B USD이상이 되는 국가만을 구해서 화면에 출력해야 합니다.
print(df[df['GDP_USD_billion'] >= 100])

##### 각 Region별로 top5 국가의 GDP 평균을 구해서 화면에 출력해야 합니다.
region_top5 = df.sort_values(by=['GDP_USD_billion'], ascending=False).groupby('region').head(5)
print(region_top5.groupby('region')['GDP_USD_billion'].mean())

#----------------------------------------------------------------
# 화면 출력2
##### SQL Query를 사용해야 합니다.
def sendQuery(sql):
    try:
        with sqlite3.connect("missions/W1/M3/World_Economies.db") as conn:
            cursor = conn.cursor() #커서 오브젝트 생성
            cursor.execute(sql)
            conn.commit() #변경사항 저장
            return cursor.fetchall() 
        
    except sqlite3.Error as e:
        print(e)

##### GDP가 100B USD이상이 되는 국가만을 구해서 화면에 출력해야 합니다.
sql = """SELECT Country, GDP_USD_billion FROM Countries_by_GDP
WHERE GDP_USD_billion >= 100
"""
print(sendQuery(sql))

##### 각 Region별로 top5 국가의 GDP 평균을 구해서 화면에 출력해야 합니다.
sql = """
SELECT region, AVG(GDP_USD_billion) AS avg_gdp_top5
FROM (
  SELECT region, GDP_USD_billion,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY GDP_USD_billion DESC) AS rnk
  FROM Countries_by_GDP
) AS ranked
WHERE rnk <= 5
GROUP BY region;"""
print(sendQuery(sql))


#----------------------------------------------------------------
# 팀 활동 요구사항
##### wikipidea 페이지가 아닌, IMF 홈페이지에서 직접 데이터를 가져오는 방법은 없을까요? 어떻게 하면 될까요?
##### 만약 데이터가 갱신되면 과거의 데이터는 어떻게 되어야 할까요? 과거의 데이터를 조회하는 게 필요하다면 ETL 프로세스를 어떻게 변경해야 할까요?

#1. 윤석님이 보내주신 것처럼 IMF 홈페이지 자체에서 공식적으로 제공하는 api와 json파일을 이용하면 된다
#2. 데이터베이스도 flyway와 같은 형상 관리 툴이 있다고 한다. 이런것을 이용하면 되지 않을까했는데, ETL 프로세스 자체를 변경해서 과거 데이터를 조회하려면... 새로운 데이터 로드 시에 기존 데이터를 덮어쓰지 않고 저장해둬야되겠다. 지금 보려고 하는 데이터는 GDP니까 GDP 데이터의 예측 시점을 기준으로 테이블에 새로운 데이터를 붙여나가야 할것 같다.
# 찾아보니 시계열 데이터를 위한 시계열 데이터베이스 같은 경우는 아키텍쳐 디자인이 다르다고 한다. time-stamp기반으로 데이터를 압축, 요약하여 저장한다. 시간 기준으로 아예 파티션을 나눠둔다. 
# 팀원의 아이디어 - GDP같은 경우는 나라 별로 연도에 따라 큰 변동이 없을 것임, 큰 변동이 나타난 나라에 대한 정보만 저장하면 저장할 데이터량이 줄어들 것이다. 