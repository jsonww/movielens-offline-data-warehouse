CREATE DATABASE IF NOT EXISTS ml_ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ads.ads_year_users_cnt (
    year                    STRING  COMMENT '年度',
    annual_users_cnt        INT     COMMENT '年度用户数',
    annual_rating_users_cnt INT     COMMENT '年度评分用户数',
    annual_tag_users_cnt    INT     COMMENT '年度标签用户数'
) COMMENT '每年用户数量'
STORED AS PARQUET
LOCATION '/warehouse/ml/ads/ads_year_users_cnt/';

INSERT OVERWRITE TABLE ml_ads.ads_year_users_cnt
SELECT
    CONCAT(year, '-01-01') AS year,
    annual_users_cnt,
    annual_rating_users_cnt,
    annual_tag_users_cnt
FROM
    ml_dwt.dwt_year_info;