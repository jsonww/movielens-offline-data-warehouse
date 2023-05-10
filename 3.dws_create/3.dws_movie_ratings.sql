CREATE DATABASE IF NOT EXISTS ml_dws;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dws.dws_movie_ratings (
    movie_id            INT     COMMENT '电影id',
    movie_ratings_dist  STRING  COMMENT '电影评分分布'
) COMMENT '电影评分分布表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dws/dws_movie_ratings/';

INSERT OVERWRITE TABLE ml_dws.dws_movie_ratings
SELECT
    movie_id,
    CONCAT_WS(' | ', COLLECT_LIST(rc)) AS ratings_dist
FROM
(
    SELECT
        movie_id,
        CONCAT(rating, '(', rating_cnt, ')') AS rc
    FROM
    (
        SELECT
            movie_id,
            rating,
            COUNT(*) AS rating_cnt
        FROM
            ml_dwd.dwd_ratings
        GROUP BY
            movie_id,
            rating
    ) AS tmrc
) AS tmrcrc
GROUP BY
    movie_id;