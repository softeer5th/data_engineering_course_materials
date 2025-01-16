# Softeer Data Engineering WIKI

## 개요

Softeer Data Engineering 코스를 수강하며 수행한 과제와
학습 내용을 정리한 저장소입니다.

## 디렉터리 구조

```
softeer
├─ .python-version
├─ README.md
├─ missions
│  └─ W1
│     ├─ M1
│     │  ├─ column_info.md
│     │  ├─ data_exploration.ipynb
│     │  └─ mtcars.csv
│     ├─ M2
│     │  ├─ learning_sql.ipynb
│     │  └─ test.db
│     └─ M3
│        ├─ data
│        │  ├─ Countries_by_GDP.json
│        │  ├─ Countries_by_GDP_etl_processed.json
│        │  ├─ World_Economies.db
│        │  ├─ api
│        │  │  └─ extracted
│        │  │     ├─ gdp_2019.parquet
│        │  │     ├─ gdp_2020.parquet
│        │  │     ├─ gdp_2021.parquet
│        │  │     ├─ gdp_2022.parquet
│        │  │     ├─ gdp_2023.parquet
│        │  │     └─ gdp_2024.parquet
│        │  └─ country_regions.json
│        ├─ etl_project_gdp.py
│        ├─ etl_project_gdp_using_api.py
│        ├─ etl_project_gdp_visualization.ipynb
│        ├─ etl_project_gdp_with_sql.py
│        ├─ etl_project_gdp_with_sql_visualization.ipynb
│        ├─ log
│        │  ├─ etl_project_log.txt
│        │  ├─ etl_project_log_etl_processed.txt
│        │  └─ etl_project_log_using_api.txt
│        ├─ processor
│        │  ├─ __init__.py
│        │  ├─ api
│        │  │  ├─ __init__.py
│        │  │  ├─ extractor.py
│        │  │  └─ transformer.py
│        │  ├─ extractor.py
│        │  ├─ io_handler.py
│        │  ├─ json_loader.py
│        │  ├─ sqlite_loader.py
│        │  └─ transformer.py
│        └─ utils
│           ├─ __init__.py
│           └─ logging.py
├─ poetry.lock
├─ pyproject.toml
├─ retrospect
│  ├─ 2025-01-02.md
│  ├─ 2025-01-03.md
│  ├─ 2025-01-06.md
│  ├─ 2025-01-08.md
│  ├─ 2025-01-09.md
│  └─ 2525-01-07.md
├─ self_study
│  ├─ README.md
│  ├─ learning_pandas
│  │  ├─ data.csv
│  │  └─ pandas.ipynb
│  └─ parallellism
│     ├─ files
│     │  ├─ pool_test.py
│     │  └─ queue_test.py
│     ├─ testing_does_pandas_use_multicore.ipynb
│     └─ testing_multiprocessing.ipynb
└─ slides
   ├─ W1 Introduction to Data Engineering.pdf
   ├─ W2 Introduction to Big Data.pdf
   ├─ W3 Introduction to Apache Hadoop.pdf
   ├─ W4 Introduction to Apache Spark.pdf
   ├─ W5 How Spark Works Internally - RDD and DAG.pdf
   ├─ W6 Optimizing Spark Job.pdf
   ├─ W7 Monitoring and Optimizing Spark Job.pdf
   └─ W8 Adaptive Query Execution in Spark.pdf

```