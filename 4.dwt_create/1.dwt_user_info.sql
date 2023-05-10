CREATE DATABASE IF NOT EXISTS ml_dwt;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dwt.dwt_user_info (
    user_id                     INT     COMMENT '用户id',
    user_years_n                INT     COMMENT '几年活动记录',
    user_avg_ratings            FLOAT   COMMENT '用户平均评分',
    user_ratings_cnt_all        INT     COMMENT '用户历史评分数',
    user_ratings_cnt_last1year  INT     COMMENT '用户最近1年评分数',
    user_ratings_cnt_last3year  INT     COMMENT '用户最近3年评分数',
    user_min_rating_time        STRING  COMMENT '用户最早评分时间',
    user_max_rating_time        STRING  COMMENT '用户最近评分时间',
    user_tags_cnt_all	        INT     COMMENT '用户历史标签数量',
    user_tags_cnt_last1year	    INT     COMMENT '用户最近1年标签数量',
    user_tags_cnt_last3year	    INT     COMMENT '用户最近3年标签数量',
	user_min_tag_time           STRING  COMMENT '用户最早标签时间',
    user_max_tag_time           STRING  COMMENT '用户最近标签时间'
) COMMENT '用户详情表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dwt/dwt_user_info/';

INSERT OVERWRITE TABLE ml_dwt.dwt_user_info
SELECT
    user_id,
    COUNT(1) AS years_n,
    ROUND(SUM(avg_ratings * ratings_cnt) / SUM(ratings_cnt), 2) AS avg_ratings,
    SUM(ratings_cnt) AS ratings_cnt_all,
    SUM(IF(last_i_year = 1, ratings_cnt, 0)) AS ratings_cnt_last1year,
    SUM(IF(last_i_year < 4, ratings_cnt, 0)) AS ratings_cnt_last3year,
    NVL(FROM_UNIXTIME(MIN(min_rating_time)), '-') AS min_rating_time,
    NVL(FROM_UNIXTIME(MAX(max_rating_time)), '-') AS max_rating_time,
    SUM(tags_cnt) AS tags_cnt_all,
    SUM(IF(last_i_year = 1, tags_cnt, 0)) AS tags_cnt_last1year,
    SUM(IF(last_i_year < 4, tags_cnt, 0)) AS tags_cnt_last3year,
	NVL(FROM_UNIXTIME(MIN(min_tag_time)), '-') AS min_tag_time,
    NVL(FROM_UNIXTIME(MAX(max_tag_time)), '-') AS max_tag_time
FROM
    ml_dws.dws_users_rc_year
GROUP BY
    user_id;