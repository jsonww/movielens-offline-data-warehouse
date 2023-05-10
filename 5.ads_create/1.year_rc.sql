CREATE DATABASE IF NOT EXISTS ml_ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ads.ads_year_rc (
    year                STRING  COMMENT '年度',
    annual_avg_ratings  FLOAT   COMMENT '年度平均评分',
    cumsum_avg_ratings  FLOAT   COMMENT '历史平均评分',
    annual_ratings_cnt  INT     COMMENT '年度评分数',
    cumsum_ratings_cnt  INT     COMMENT '历史评分数',
    annual_tags_cnt     INT     COMMENT '年度标签数',
    cumsum_tags_cnt     INT     COMMENT '历史标签数'
) COMMENT '每年评分标签数'
STORED AS PARQUET
LOCATION '/warehouse/ml/ads/ads_year_rc/';

INSERT OVERWRITE TABLE ml_ads.ads_year_rc
SELECT
    -- CAST(CONCAT(year, '-01-01') AS DATE) AS year,
    CONCAT(year, '-01-01') AS year,
    annual_avg_ratings,
    cumsum_avg_ratings,
    annual_ratings_cnt,
    cumsum_ratings_cnt,
    annual_tags_cnt,
    cumsum_tags_cnt
FROM
    ml_dwt.dwt_year_info;