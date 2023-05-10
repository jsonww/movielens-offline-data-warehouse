CREATE DATABASE IF NOT EXISTS ml_dws;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dws.dws_users_rc_year (
    last_i_year		TINYINT    	COMMENT '最近第几年',
	year        	SMALLINT    COMMENT '年份',
    user_id     	INT         COMMENT '用户id',
    ratings_cnt 	INT			COMMENT '评分数',
    avg_ratings		FLOAT      	COMMENT '评分',
    min_rating_time	BIGINT      COMMENT '最早评分时间',
    max_rating_time	BIGINT      COMMENT '最近评分时间',
    tags_cnt 		INT			COMMENT '标签数',
	min_tag_time	BIGINT      COMMENT '最早标签时间',
    max_tag_time	BIGINT      COMMENT '最近标签时间'
) COMMENT '用户历年评分标签汇总'
STORED AS PARQUET
LOCATION '/warehouse/ml/dws/dws_users_rc_year/';

WITH
tur AS (
	SELECT 
		FROM_UNIXTIME(rating_time, 'yyyy') AS year,
		user_id,
		COUNT(*) AS ratings_cnt, 
		ROUND(AVG(rating), 2) AS avg_ratings,
        MIN(rating_time) AS min_rating_time,
        MAX(rating_time) AS max_rating_time
	FROM 
		ml_dwd.dwd_ratings
	GROUP BY
		FROM_UNIXTIME(rating_time, 'yyyy'),
		user_id
),
tut AS (
	SELECT 
		FROM_UNIXTIME(tag_time, 'yyyy') AS year,
		user_id,
		COUNT(*) AS tags_cnt,
        MIN(tag_time) AS min_tag_time,
        MAX(tag_time) AS max_tag_time 
	FROM 
		ml_dwd.dwd_tags
	GROUP BY
		FROM_UNIXTIME(tag_time, 'yyyy'),
		user_id 
)
INSERT overwrite TABLE ml_dws.dws_users_rc_year
SELECT
	ROW_NUMBER() OVER (PARTITION BY res.user_id ORDER BY res.year DESC) AS last_i_year,
	res.*
FROM
(
	SELECT
		COALESCE(tur.year, tut.year) AS year,
		COALESCE(tur.user_id, tut.user_id) AS user_id,
		NVL(tur.ratings_cnt, 0) AS ratings_cnt,
		NVL(tur.avg_ratings, 0) AS avg_ratings,
		tur.min_rating_time AS min_rating_time,
		tur.max_rating_time AS max_rating_time,
		NVL(tut.tags_cnt, 0) AS tags_cnt,
		tut.min_tag_time AS min_tag_time,
		tut.max_tag_time AS max_tag_time
	FROM
		tur
	FULL OUTER JOIN
		tut
	ON
		tur.year = tut.year AND tur.user_id = tut.user_id
) AS res;