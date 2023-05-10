CREATE DATABASE IF NOT EXISTS ml_ods;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ods.ods_tags(
    userId  INT     COMMENT '用户id',
    movieId INT     COMMENT '电影id',
    tag     STRING  COMMENT '标签',
    times   BIGINT  COMMENT '标签时间戳'
) COMMENT '标签表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/warehouse/ml/ods/tags'
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH "/upload/ml-25m/tags.csv"
INTO TABLE ml_ods.ods_tags;