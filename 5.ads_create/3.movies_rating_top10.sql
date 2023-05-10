
CREATE DATABASE IF NOT EXISTS ml_ads;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_ads.ads_movies_rating_top10 (
    movie_id            INT     COMMENT '电影id',
    title               STRING  COMMENT '电影名称',
    genres              STRING  COMMENT '电影类型',
    movie_avg_ratings   FLOAT   COMMENT '电影平均评分',
    movie_ratings_cnt   INT     COMMENT '电影评分数',
    movie_ratings_dist  STRING  COMMENT '电影评分分布'
) COMMENT '评分数top10的电影'
STORED AS PARQUET
LOCATION '/warehouse/ml/ads/ads_movies_rating_top10/';

INSERT OVERWRITE TABLE ml_ads.ads_movies_rating_top10
SELECT
    tm.movie_id,
    tdm.title,
    tdm.genres,
    tm.movie_avg_ratings,
    tm.movie_ratings_cnt,
    tm.movie_ratings_dist
    -- CONCAT('http://www.imdb.com/title/', tdm.imdb_id) AS imdb_link,
    -- CONCAT('https://www.themoviedb.org/movie/'
FROM
(
    SELECT
        movie_id,
        movie_avg_ratings,
        movie_ratings_cnt,
        movie_ratings_dist,
        movie_tags_cnt,
        movie_tags_dist
    FROM
        ml_dwt.dwt_movie_info
    ORDER BY
        movie_ratings_cnt DESC
    LIMIT 10
) AS tm 
LEFT JOIN
    ml_dim.dim_movies AS tdm
ON
    tm.movie_id = tdm.movie_id;