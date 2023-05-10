CREATE DATABASE IF NOT EXISTS ml_dwd;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dwd.dwd_tags(
    user_id     INT     COMMENT '用户id',
    movie_id    INT     COMMENT '电影id',
    tag         STRING  COMMENT '标签',
    tag_time    BIGINT  COMMENT '标签时间戳'
) COMMENT '标签表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dwd/dwd_tags/';

INSERT OVERWRITE TABLE ml_dwd.dwd_tags
SELECT
    userId AS user_id,
    movieId AS movie_id,
    tag,
    times AS tag_time
FROM
    ml_ods.ods_tags;