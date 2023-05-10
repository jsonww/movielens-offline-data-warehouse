CREATE DATABASE IF NOT EXISTS ml_ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ods.ods_ratings(
    userId INT      COMMENT '用户id',
    movieId INT     COMMENT '电影id',
    rating FLOAT    COMMENT '评分',
    times BIGINT    COMMENT '评分时间戳'
) COMMENT '评分表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/warehouse/ml/ods/ratings'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH "/upload/ml-25m/ratings.csv"
INTO TABLE ml_ods.ods_ratings;