### 개요
wikipedia로부터 표를 읽어오거나 IMF로부터 gdp데이터를 받아와서 gdp가 100B$ 이상인 국가의 리스트를 출력하고, 지역별 GDP 상위 5개국의 평균 GDP를 출력하는 프로그램을 작성하였다.

### ETL단계
||단계|wikipedia|IMF|
|-|--|---------------------|-----------------|
|E|추출|wikipedia html table|IMF API json data|
|T|변환|html->pandas dataFrame|json->pandas dataFrame|
|L|로드|json 파일 출력|데이터 베이스 저장|

위와 같이 ETL 단계를 정의했고 이에 따라 프로그램을 작성하였다.

* etl_project_gdp.py:

![etl flow from wikipedia to json](https://github.com/tlsdbstjr/DataEngineering/blob/main/wiki_image/W1/wikipedia%20etl%20flow.png?raw=true)

> * 이름이 같은 작업은 같은 함수이다.
> * 데이터 가공은 pandas로 진행한다. 사실 load 과정 없이도 작동하는데 지장이 없다.

* etl_project_gdp_with_sql.py:

![elt flow from IMF api to database](https://github.com/tlsdbstjr/DataEngineering/blob/main/wiki_image/W1/IMF%20etl%20flow.png?raw=true)

> * 이름이 같은 작업은 같은 함수이다.
> * 데이터 가공은 sql로 진행한다. ETL보다는 ELT에 가까운 작동 방식이다.
> * 이 파일은 사용자 UI가 적용되어 있다 다음과 같은 순으로 키보드 입력이 필요하다:
>   * 조회할 년도
>   * 조회할 지역 구분
>   * 조회할 지역 (0을 입력하면 전세계, 한 지역을 입력하면 상위 국가 GDP도 출력)