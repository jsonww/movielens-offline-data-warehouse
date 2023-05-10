CREATE DATABASE IF NOT EXISTS ml_dws;

CREATE EXTERNAL TABLE IF NOT EXISTS ml_dws.dws_movie_tags (
    movie_id    	INT     COMMENT '电影id',
    movie_tags_dist	STRING  COMMENT '电影标签分布'
) COMMENT '电影标签集合表'
STORED AS PARQUET
LOCATION '/warehouse/ml/dws/dws_movie_tags/';

INSERT OVERWRITE TABLE ml_dws.dws_movie_tags
SELECT
	movie_id,
	CONCAT_WS(' | ', COLLECT_SET(tc)) AS tags_dist
FROM
(
	SELECT
		movie_id,
		CONCAT(tag, '(', tag_cnt, ')') AS tc
	FROM
	(   
		SELECT
			movie_id,
			tag,
			COUNT(*) AS tag_cnt
		FROM
			ml_dwd.dwd_tags
		GROUP BY
			movie_id,
			tag
		ORDER BY
			movie_id ASC,
			tag_cnt DESC
	) AS tttc
) AS ttc
GROUP BY
	movie_id;