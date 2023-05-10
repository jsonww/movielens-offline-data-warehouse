CREATE DATABASE IF NOT EXISTS ml_ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ads.ads_users_rating_top100 (
    user_id                     INT     COMMENT '用户id',
    user_avg_ratings            FLOAT   COMMENT '用户平均评分',
    user_last_rating_time       STRING  COMMENT '用户最近评分时间',
    user_ratings_cnt_all        INT     COMMENT '用户历史评分数',
    user_ratings_cnt_last1year  INT     COMMENT '用户最近1年评分数',
    user_ratings_cnt_last3year  INT     COMMENT '用户最近3年评分数',
    user_last_tag_time          STRING  COMMENT '用户最近标签时间',
    user_tags_cnt_all	        INT     COMMENT '用户历史标签数量',
    user_tags_cnt_last1year	    INT     COMMENT '用户最近1年标签数量',
    user_tags_cnt_last3year	    INT     COMMENT '用户最近3年标签数量'
) COMMENT '平分数前100的用户'
STORED AS PARQUET
LOCATION '/warehouse/ml/ads/ads_users_rating_top100/';

INSERT OVERWRITE TABLE ml_ads.ads_users_rating_top100
SELECT
    user_id,
    user_avg_ratings,
    user_max_rating_time,
    user_ratings_cnt_all,
    user_ratings_cnt_last1year,
    user_ratings_cnt_last3year,
    user_max_tag_time,
    user_tags_cnt_all,
    user_tags_cnt_last1year,
    user_tags_cnt_last3year
FROM
    ml_dwt.dwt_user_info
ORDER BY
    user_ratings_cnt_all DESC
LIMIT 100;