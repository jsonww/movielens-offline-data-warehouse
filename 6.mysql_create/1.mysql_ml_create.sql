CREATE DATABASE IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.ads_year_rc (
    year                DATETIME    COMMENT '年度',
    annual_avg_ratings  FLOAT       COMMENT '年度平均评分',
    cumsum_avg_ratings  FLOAT       COMMENT '历史平均评分',
    annual_ratings_cnt  INT         COMMENT '年度评分数',
    cumsum_ratings_cnt  INT         COMMENT '历史评分数',
    annual_tags_cnt     INT         COMMENT '年度标签数',
    cumsum_tags_cnt     INT         COMMENT '历史标签数'
) COMMENT '每年评分标签状况';

CREATE TABLE IF NOT EXISTS ml.ads_genres_cnt (
    genre               CHAR(20)    COMMENT '电影类型',
    genre_movies_cnt    INT         COMMENT '该类型电影数量',
    genre_avg_ratings   FLOAT       COMMENT '该类型平均评分',
    genre_ratings_cnt   INT         COMMENT '该类型电影评分数量',
    genre_tags_cnt      INT         COMMENT '该类型电影标签数量'
) COMMENT '电影类型数量/评分/标签汇总';

CREATE TABLE IF NOT EXISTS ml.ads_movies_rating_top10 (
    movie_id            INT             COMMENT '电影id',
    title               CHAR(200)       COMMENT '电影名称',
    genres              CHAR(200)       COMMENT '电影类型',
    movie_avg_ratings   FLOAT           COMMENT '电影平均评分',
    movie_ratings_cnt   INT             COMMENT '电影评分数',
    movie_ratings_dist  VARCHAR(200)    COMMENT '电影评分分布'
) COMMENT '评分数top10电影';

CREATE TABLE IF NOT EXISTS ml.ads_year_users_cnt (
    year                    DATETIME    COMMENT '年度',
    annual_users_cnt        INT         COMMENT '年度用户数',
    annual_rating_users_cnt INT         COMMENT '年度评分用户数',
    annual_tag_users_cnt    INT         COMMENT '年度标签用户数'
) COMMENT '每年用户数量';

CREATE TABLE IF NOT EXISTS ml.ads_year_users_status (
    year                    INT COMMENT '年度',
    first_use_users_cnt     INT COMMENT '当年第一次使用用户数',
    recent_use_users_cnt    INT COMMENT '当年最近一次使用用户数'
) COMMENT '每年用户状态';

CREATE TABLE IF NOT EXISTS ml.ads_users_rating_top100 (
    user_id                     INT         COMMENT '用户id',
    user_avg_ratings            FLOAT       COMMENT '用户平均评分',
    user_last_rating_time       DATETIME    COMMENT '用户最近评分时间',
    user_ratings_cnt_all        INT         COMMENT '用户历史评分数',
    user_ratings_cnt_last1year  INT         COMMENT '用户最近1年评分数',
    user_ratings_cnt_last3year  INT         COMMENT '用户最近3年评分数',
    user_last_tag_time          DATETIME    COMMENT '用户最近标签时间',
    user_tags_cnt_all	        INT         COMMENT '用户历史标签数量',
    user_tags_cnt_last1year	    INT         COMMENT '用户最近1年标签数量',
    user_tags_cnt_last3year	    INT         COMMENT '用户最近3年标签数量'
) COMMENT '评分数前100用户';