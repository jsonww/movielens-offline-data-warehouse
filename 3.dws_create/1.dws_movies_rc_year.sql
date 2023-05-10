CREATE DATABASE IF NOT EXISTS ml_dws;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dws.dws_movies_rc_year (
    year        	SMALLINT    COMMENT '年份',
    movie_id    	INT         COMMENT '电影id',
    ratings_cnt 	INT			COMMENT '评分数',
    avg_ratings		FLOAT      	COMMENT '评分',
	min_rating_time	BIGINT      COMMENT '最早评分时间',
    max_rating_time	BIGINT      COMMENT '最近评分时间',
    tags_cnt		INT      	COMMENT '标签数',
	min_tag_time	BIGINT      COMMENT '最早标签时间',
    max_tag_time	BIGINT      COMMENT '最近标签时间'
) COMMENT '电影历年评分标签汇总'
STORED AS PARQUET
LOCATION '/warehouse/ml/dws/dws_movies_rc_year/';

WITH
tmr AS (
	SELECT 
		FROM_UNIXTIME(rating_time, 'yyyy') AS year,
		movie_id,
		COUNT(*) AS ratings_cnt, 
		ROUND(AVG(rating), 2) AS avg_ratings,
        MIN(rating_time) AS min_rating_time,
        MAX(rating_time) AS max_rating_time
	FROM 
		ml_dwd.dwd_ratings
	GROUP BY
		FROM_UNIXTIME(rating_time, 'yyyy'),
		movie_id
),
tmt AS (
	SELECT 
		FROM_UNIXTIME(tag_time, 'yyyy') AS year,
		movie_id,
		COUNT(*) AS tags_cnt,
        MIN(tag_time) AS min_tag_time,
        MAX(tag_time) AS max_tag_time
	FROM 
		ml_dwd.dwd_tags
	GROUP BY
		FROM_UNIXTIME(tag_time, 'yyyy'),
		movie_id 
)
INSERT overwrite TABLE ml_dws.dws_movies_rc_year
SELECT
	COALESCE(tmr.year, tmt.year) AS year,
	COALESCE(tmr.movie_id, tmt.movie_id) AS movie_id,
	NVL(tmr.ratings_cnt, 0) AS ratings_cnt,
	NVL(tmr.avg_ratings, 0) AS avg_ratings,
    tmr.min_rating_time,
    tmr.max_rating_time,
	NVL(tmt.tags_cnt, 0) AS tags_cnt,
	tmt.min_tag_time,
    tmt.max_tag_time
FROM
	tmr
FULL OUTER JOIN
	tmt
ON
	tmr.year = tmt.year AND tmr.movie_id = tmt.movie_id;