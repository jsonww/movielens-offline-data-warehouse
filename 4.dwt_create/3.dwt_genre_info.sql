CREATE DATABASE IF NOT EXISTS ml_dwt;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dwt.dwt_genre_info (
    genre                   STRING  COMMENT '电影类型',
    genre_movies_cnt        INT     COMMENT '该电影类型电影数量',
    genre_avg_ratings       FLOAT   COMMENT '该电影类型平均评分',
    genre_ratings_cnt       INT     COMMENT '该电影类型评分数',
    genre_min_rating_time   STRING  COMMENT '该电影类型最早评分时间',
    genre_max_rating_time   STRING  COMMENT '该电影类型最近评分时间',
	genre_tags_cnt	        INT     COMMENT '该电影类型标签数量',
	genre_min_tag_time      STRING  COMMENT '该电影类型最早标签时间',
    genre_max_tag_time      STRING  COMMENT '该电影类型最近标签时间'
) COMMENT '电影类型详情表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dwt/dwt_genre_info/';

INSERT OVERWRITE TABLE ml_dwt.dwt_genre_info
SELECT
    tmg.genre,
    COUNT(DISTINCT tmrc.movie_id) AS movies_cnt,
    ROUND(SUM(tmrc.ratings_cnt * tmrc.avg_ratings) / SUM(tmrc.ratings_cnt), 2) AS avg_ratings,
    SUM(tmrc.ratings_cnt) AS ratings_cnt,
    NVL(FROM_UNIXTIME(MIN(tmrc.min_rating_time)), '-') AS min_rating_time,
    NVL(FROM_UNIXTIME(MAX(tmrc.max_rating_time)), '-') AS max_rating_time,
    SUM(tmrc.tags_cnt) AS tags_cnt,
    NVL(FROM_UNIXTIME(MIN(tmrc.min_tag_time)), '-') AS min_tag_time,
    NVL(FROM_UNIXTIME(MAX(tmrc.max_tag_time)), '-') AS max_tag_time
FROM
    ml_dws.dws_movies_rc_year AS tmrc
LEFT JOIN
    ml_dim.dim_movies_genres AS tmg
ON
    tmrc.movie_id = tmg.movie_id
GROUP BY
    tmg.genre;
    