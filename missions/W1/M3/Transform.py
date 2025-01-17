import pandas as pd
import datetime

# '-' 처리 함수
def remove_bar(df, log_file):
    mask = df['GDP_USD_million'] == '—' 
    df.loc[mask, ['GDP_USD_million', 'year']] = None  #GDP가 '-'라면 GDP와 year은 None으로 처리
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Transform) 데이터 결측치 처리", file=log_file)
    return df

#위키피디아 주석 제거 함수
def remove_wiki_annotations(df, log_file):
    df['year'] = df['year'].str.replace(r'\[.*?\]', '', regex=True).str.strip()  # 'year' 열에만 적용
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Transform) 불필요한 주석 제거", file=log_file)
    return df


#단위 변경 함수(million -> billion)
def million_to_billion(df, log_file):
    df.insert(
        df.columns.get_loc('GDP_USD_million') + 1,  'GDP_USD_billion',  # 'GDP_USD_million' 열의 바로 다음 위치에 'GDP_USD_billion' 컬럼 추가
        df['GDP_USD_million'].str.replace(',', '', regex=False)  # 쉼표 제거
        .astype(float, errors='ignore')  # 문자열을 float로 변환
        .div(1000).round(2)  # billion 단위로 변환
    )
    print(datetime.datetime.now().strftime('%Y-%B-%d-%H-%M-%S,'), "(Transform) 데이터 단위 변경", file=log_file)
    return df