CREATE DATABASE IF NOT EXISTS ml_ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ads.ads_genres_cnt (
    genre               STRING  COMMENT '电影类型',
    genre_movies_cnt    INT     COMMENT '该类型电影数量',
    genre_avg_ratings   FLOAT   COMMENT '该类型平均评分',
    genre_ratings_cnt   INT     COMMENT '该类型电影评分数量',
    genre_tags_cnt      INT     COMMENT '该类型电影标签数量'
) COMMENT '电影类型数量'
STORED AS PARQUET
LOCATION '/warehouse/ml/ads/ads_genres_cnt/';

INSERT OVERWRITE TABLE ml_ads.ads_genres_cnt
SELECT
    genre,
    genre_movies_cnt,
    genre_avg_ratings,
    genre_ratings_cnt,
    genre_tags_cnt
FROM
    ml_dwt.dwt_genre_info;