CREATE DATABASE IF NOT EXISTS ml_dim;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dim.dim_movies_genres(
    movie_id    INT      COMMENT '电影id',
    genre       STRING   COMMENT '电影类型'
) COMMENT '电影类型表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dim/dim_movies_genres/';

INSERT OVERWRITE TABLE ml_dim.dim_movies_genres
SELECT
    movieId AS movie_id,
    gen AS genre
FROM
    ml_ods.ods_movies AS m
LATERAL VIEW
    EXPLODE(SPLIT(m.genres, '\\|')) gen_tmp AS gen;  -- 套了一层 shell 所以 ‘|’ 需要转义