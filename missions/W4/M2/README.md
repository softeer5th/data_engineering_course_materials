# README: NYC Taxi and Weather Data Analysis

## 1. 사용한 데이터 설명

### 1.1 NYC Yellow Taxi 데이터

- **출처:** NYC Taxi & Limousine Commission [(TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **설명:** 뉴욕시 5개 자치구(Manhattan, Brooklyn, Queens, Bronx, Staten Island)에서 운행된 Yellow Taxi의 이동 데이터를 포함함.
- **사용한 데이터 범위:** 2021년 1월 ~ 12월
- **주요 컬럼(전처리 후):**
  - `tpep_pickup_datetime`: 택시 승차(픽업) 시간
  - `pickup_hour`: 픽업 시간 (시간 단위 추출)
  - `trip_distance`: 이동 거리 (마일)
  - `trip_duration_min`: 이동 시간 (분 단위)
  - `total_amount`: 승객이 지불한 총 금액 (요금 + 팁 + 기타 비용)
  - `PULocationID`, `DOLocationID`: 픽업 및 드롭오프 지역 ID (TLC 지정)

### 1.2 NewYork Weather 데이터

- **출처:** Kaggle (Albany, New York 기후 데이터) [(Kaggle)](https://www.kaggle.com/datasets/die9origephit/temperature-data-albany-new-york/data)
- **설명:** Albany(뉴욕의 도시) 기상 관측소에서 수집한 시간별 기후 데이터. 뉴욕시의 기후 트렌드를 대체적으로 반영할 수 있음.
- **사용한 데이터 범위:** 2021년 1월 ~ 12월
- **주요 컬럼:**
  - `DATE`: 관측 날짜
  - `weather_datetime`: 관측 시간 (UTC 기준)
  - `temperature`: 기온 (화씨)
  - `humidity`: 상대 습도 (%)
  - `precipitation`: 강수량 (인치)
  - `wind_speed`: 풍속 (mph)

## 2. 데이터 처리 과정

1. **데이터 로드 및 전처리**
   - NYC Yellow Taxi 데이터와 Albany Weather 데이터를 로드하여 날짜/시간을 변환하고, 필요한 컬럼을 선택하여 정리함.
   - 기후 데이터의 `weather_datetime`과 택시 데이터의 `pickup_datetime`을 기준으로 병합.

2. **데이터 샘플링**
   - 전체 데이터의 1%를 샘플링하여 분석 수행 (샘플 데이터 크기 조절 가능).

3. **상관관계 분석**
   - 기후 요소(기온, 습도, 강수량, 풍속)와 이동 거리 및 픽업 횟수 간의 상관관계를 분석함.

4. **시각화**
   - 날씨 요소별 택시 이동 거리 및 픽업 횟수를 그래프로 표현하여 기후와 택시 이용 패턴 간의 관계를 확인함.

## 3. 주요 분석 목표
- **기온이 높을수록 택시 픽업이 증가하는가?**
- **습도가 높을수록 택시 이용량이 증가하는가?**
- **비가 오는 날과 맑은 날의 택시 이용 패턴 차이는 존재하는가?**
- **풍속이 증가하면 택시 이용이 줄어드는가?**

분석을 통해 날씨가 택시 이용량에 미치는 영향을 알아내고, 관련 인사이트를 도출하는 것이 목표

