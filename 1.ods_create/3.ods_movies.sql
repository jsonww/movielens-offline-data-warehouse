CREATE DATABASE IF NOT EXISTS ml_ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ods.ods_movies(
    movieId INT     COMMENT '电影id',
    title   STRING  COMMENT '电影标题',
    genres  STRING  COMMENT '电影类型合集'
) COMMENT '电影表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/warehouse/ml/ods/movies'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH "/upload/ml-25m/movies.csv"
INTO TABLE ml_ods.ods_movies;

-- SELECT
--     movieId,
--     gen
-- FROM
-- (
--     SELECT
--         movieId,
--         `genres`
--     FROM
--         ml_ods.ods_movies
--     WHERE
--         movieId = 1
-- ) AS res
-- LATERAL VIEW
--     EXPLODE(SPLIT(genres, '\\|')) gen_tmp AS gen;