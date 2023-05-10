CREATE DATABASE IF NOT EXISTS ml_dwt;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dwt.dwt_year_info (
    year                        SMALLINT    COMMENT '年度',
    annual_rating_movies_cnt    INT         COMMENT '年度被评分电影数',
    annual_ratings_cnt          INT         COMMENT '年度评分次数',
    cumsum_ratings_cnt          INT         COMMENT '历史评分次数',
    annual_avg_ratings          FLOAT       COMMENT '年度平均评分',
    cumsum_avg_ratings          FLOAT       COMMENT '历史平均评分',
    annual_tag_movies_cnt       INT         COMMENT '年度被标签电影数',
    annual_tags_cnt             INT         COMMENT '年度标签次数',
    cumsum_tags_cnt             INT         COMMENT '历史标签次数',
    annual_users_cnt            INT         COMMENT '年度用户数',
    annual_rating_users_cnt     INT         COMMENT '年度评分用户数',
    annual_rating_users_percent FLOAT       COMMENT '年度评分用户数占比',
    annual_tag_users_cnt        INT         COMMENT '年度标签用户数',
    annual_tag_users_percent    FLOAT       COMMENT '年度标签用户数占比',
    first_use_users_cnt         INT         COMMENT '当年第一次使用的用户数',
    recent_use_users_cnt        INT         COMMENT '当年最后一次使用的用户数'
) COMMENT '历年详情表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dwt/dwt_year_info/';

WITH
tyc AS (
    SELECT
        year,
        annual_rating_movies_cnt,
        annual_ratings_cnt,
        SUM(annual_ratings_cnt) OVER (ORDER BY year ASC) AS cumsum_ratings_cnt,
        annual_sum_ratings,
        SUM(annual_sum_ratings) OVER (ORDER BY year ASC) AS cumsum_sum_ratings,
        annual_tag_movies_cnt,
        annual_tags_cnt,
        SUM(annual_tags_cnt) OVER (ORDER BY year ASC) AS cumsum_tags_cnt
    FROM
    (
        SELECT
            year,
            COUNT(IF(ratings_cnt > 0, True, NULL)) AS annual_rating_movies_cnt,
            SUM(ratings_cnt) AS annual_ratings_cnt,
            SUM(ratings_cnt * avg_ratings) AS annual_sum_ratings,
            COUNT(IF(tags_cnt > 0, True, NULL)) AS annual_tag_movies_cnt,
            SUM(tags_cnt) AS annual_tags_cnt
        FROM
            ml_dws.dws_movies_rc_year
        GROUP BY
            year
    ) AS ty
),
tu AS (
    SELECT
        year,
        annual_users_cnt,
        annual_rating_users_cnt,
        ROUND(annual_rating_users_cnt / annual_users_cnt, 2) AS annual_rating_users_percent,
        annual_tag_users_cnt,
        ROUND(annual_tag_users_cnt / annual_users_cnt, 2) AS annual_tag_users_percent
    FROM
    (
        SELECT
            year,
            COUNT(1) AS annual_users_cnt,
            COUNT(IF(ratings_cnt > 0, True, NULL)) AS annual_rating_users_cnt,
            COUNT(IF(tags_cnt > 0, True, NULL)) AS annual_tag_users_cnt
        FROM
            ml_dws.dws_users_rc_year
        GROUP BY
            year
    ) AS turc
),
tuy AS (
    SELECT
        user_id,
        MIN(year) AS first_year,
        MAX(year) AS recent_year
    FROM
        ml_dws.dws_users_rc_year
    GROUP BY
        user_id
)
INSERT OVERWRITE TABLE ml_dwt.dwt_year_info
SELECT
    tyc.year,
    tyc.annual_rating_movies_cnt,
    tyc.annual_ratings_cnt,
    tyc.cumsum_ratings_cnt,
    ROUND(tyc.annual_sum_ratings / tyc.annual_ratings_cnt, 2) AS annual_avg_ratings,
    ROUND(tyc.cumsum_sum_ratings / tyc.cumsum_ratings_cnt, 2) AS cumsum_avg_ratings,
    tyc.annual_tag_movies_cnt,
    tyc.annual_tags_cnt,
    tyc.cumsum_tags_cnt,
    tu.annual_users_cnt,
    tu.annual_rating_users_cnt,
    tu.annual_rating_users_percent,
    tu.annual_tag_users_cnt,
    tu.annual_tag_users_percent,
    NVL(tf.first_use_users_cnt, 0) AS first_use_users_cnt,
    NVL(tr.recent_use_users_cnt, 0) AS recent_use_users_cnt
FROM
    tyc
LEFT JOIN
    tu
ON
    tyc.year = tu.year
LEFT JOIN
(
    SELECT
        first_year AS year,
        COUNT(1) AS first_use_users_cnt
    FROM
        tuy
    GROUP BY
        first_year
) AS tf
ON
    tyc.year = tf.year
LEFT JOIN
(
    SELECT
        recent_year AS year,
        COUNT(1) AS recent_use_users_cnt
    FROM
        tuy
    GROUP BY
        recent_year
) AS tr
ON
    tyc.year = tr.year;