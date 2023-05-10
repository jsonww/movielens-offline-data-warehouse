CREATE DATABASE IF NOT EXISTS ml_ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ads.ads_year_users_status (
    year                    STRING  COMMENT '年度',
    first_use_users_cnt     INT     COMMENT '当年第一次使用用户数',
    recent_use_users_cnt    INT     COMMENT '当年最近一次使用用户数'
) COMMENT '每年用户状态'
STORED AS PARQUET
LOCATION '/warehouse/ml/ads/ads_year_users_status/';

INSERT OVERWRITE TABLE ml_ads.ads_year_users_status
SELECT
    year,
    first_use_users_cnt,
    recent_use_users_cnt
FROM
    ml_dwt.dwt_year_info;