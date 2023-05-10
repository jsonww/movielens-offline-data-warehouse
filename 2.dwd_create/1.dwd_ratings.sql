CREATE DATABASE IF NOT EXISTS ml_dwd;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dwd.dwd_ratings(
    user_id     INT     COMMENT '用户id',
    movie_id    INT     COMMENT '电影id',
    rating      FLOAT   COMMENT '评分',
    rating_time BIGINT  COMMENT '评分时间戳'
) COMMENT '评分表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dwd/dwd_ratings/';

INSERT OVERWRITE TABLE ml_dwd.dwd_ratings
SELECT
    userId AS user_id,
    movieId AS movie_id,
    rating,
    times AS rating_time
FROM
    ml_ods.ods_ratings;