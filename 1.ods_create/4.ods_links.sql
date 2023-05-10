CREATE DATABASE IF NOT EXISTS ml_ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ods.ods_links(
    movieId INT COMMENT '电影id',
    imdbId  INT COMMENT 'imdb链接 -- http://www.imdb.com/title/<imdbId>/.',
    tmdbId  INT COMMENT 'themoviedb链接 -- https://www.themoviedb.org/movie/<imdbId>.'
) COMMENT '链接表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/warehouse/ml/ods/links'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH "/upload/ml-25m/links.csv"
INTO TABLE ml_ods.ods_links;