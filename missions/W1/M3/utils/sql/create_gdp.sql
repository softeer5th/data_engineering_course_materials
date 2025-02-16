CREATE TABLE IF NOT EXISTS gdp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Country varchar(127) NOT NULL,
    Region varchar(127) NOT NULL, -- country가 추가되지 않고, 매핑 테이블의 변화가 없다는 가정이라면 NOT NULL
    GDP_USD_billion float,
    Year int,
    Type varchar(63) NOT NULL,
    CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);
