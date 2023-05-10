CREATE DATABASE IF NOT EXISTS ml_dwt;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dwt.dwt_movie_info (
    movie_id                INT     COMMENT '电影id',
    movie_avg_ratings       FLOAT   COMMENT '电影平均评分',
    movie_ratings_cnt       INT     COMMENT '电影评分数',
    movie_ratings_dist      STRING  COMMENT '电影评分分布',
    movie_min_rating_time   STRING  COMMENT '电影最早评分时间',
    movie_max_rating_time   STRING  COMMENT '电影最近评分时间',
	movie_tags_cnt	        INT     COMMENT '电影标签数量',
    movie_tags_dist	        STRING  COMMENT '电影标签分布',
	movie_min_tag_time      STRING  COMMENT '电影最早标签时间',
    movie_max_tag_time      STRING  COMMENT '电影最近标签时间'
) COMMENT '电影详情表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dwt/dwt_movie_info/';

INSERT OVERWRITE TABLE ml_dwt.dwt_movie_info
SELECT
    trc.movie_id,
    NVL(trc.avg_ratings, 0) AS movie_avg_ratings,
    trc.ratings_cnt,
    NVL(tr.movie_ratings_dist, '-') AS movie_ratings_dist,
    NVL(FROM_UNIXTIME(trc.min_rating_time), '-') AS min_rating_time,
    NVL(FROM_UNIXTIME(trc.max_rating_time), '-') AS max_rating_time,
    trc.tags_cnt,
    NVL(tt.movie_tags_dist, '-') AS movie_tags_dist,
	NVL(FROM_UNIXTIME(trc.min_tag_time), '-') AS min_tag_time,
    NVL(FROM_UNIXTIME(trc.max_tag_time), '-') AS max_tag_time
FROM
( 
    SELECT
        movie_id,
        ROUND(SUM(ratings_cnt * avg_ratings) / SUM(ratings_cnt), 2) AS avg_ratings,
        SUM(ratings_cnt) AS ratings_cnt,
        MIN(min_rating_time) AS min_rating_time,
        MAX(max_rating_time) AS max_rating_time,
        SUM(tags_cnt) AS tags_cnt,
	    MIN(min_tag_time) AS min_tag_time,
        MAX(max_tag_time) AS max_tag_time
    FROM
        ml_dws.dws_movies_rc_year
    GROUP BY
        movie_id
)  AS trc
LEFT JOIN
    ml_dws.dws_movie_ratings AS tr
ON
    trc.movie_id = tr.movie_id
LEFT JOIN
    ml_dws.dws_movie_tags AS tt
ON
    trc.movie_id = tt.movie_id;