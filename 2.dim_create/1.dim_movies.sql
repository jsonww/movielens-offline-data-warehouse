CREATE DATABASE IF NOT EXISTS ml_dim;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dim.dim_movies(
    movie_id    INT     COMMENT '电影id',
    title       STRING  COMMENT '电影标题',
    imdb_id     INT     COMMENT 'imdb链接 -- http://www.imdb.com/title/<imdbId>/.',
    tmdb_id     INT     COMMENT 'themoviedb链接 -- https://www.themoviedb.org/movie/<imdbId>.',
    genres      STRING  COMMENT '电影类型合集'
) COMMENT '电影维度表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dim/dim_movies/';

INSERT OVERWRITE TABLE ml_dim.dim_movies
SELECT
    m.movieId AS movie_id,
    m.title AS title,
    l.imdbId AS imdb_id,
    l.tmdbId AS tmdb_id,
    m.genres AS genres 
FROM
    ml_ods.ods_movies AS m
LEFT JOIN
    ml_ods.ods_links AS l
ON
    m.movieId = l.movieId;