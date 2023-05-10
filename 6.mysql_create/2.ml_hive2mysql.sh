#!/bin/bash

hive_db_name=ml_ads
mysql_db_name=ml

export_data() {
sqoop eval \
--connect "jdbc:mysql://hadoop102:3306/${mysql_db_name}" \
--username root \
--password iamboss \
--query "TRUNCATE TABLE $1;"

sqoop export \
--connect "jdbc:mysql://hadoop102:3306/${mysql_db_name}" \
--username root \
--password iamboss \
--table $1 \
--num-mappers 1 \
--hcatalog-database ${hive_db_name} \
--hcatalog-table $1
}

if [ $# -eq 1 ] && [ $1 = "all" ]; then
    export_data "ads_year_rc"
    export_data "ads_genres_cnt"
    export_data "ads_movies_rating_top10"
    export_data "ads_year_users_cnt"
    export_data "ads_year_users_status"
    export_data "ads_users_rating_top100"
else
    for t in $*
    do
        export_data $t
    done
fi
