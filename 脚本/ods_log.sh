#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive


if [ -n "$1" ];then
	to_date=$1
else
	to_date=`date -d "-1 day" +%F`
fi

echo "===日志日期为 $to_date==="

sql="
load data inpath '/origin_data/gmall/log/topic_start/$to_date' into table "$APP".ods_start_log partition(dt='$to_date');

load data inpath '/origin_data/gmall/log/topic_event/$to_date' into table "$APP".ods_event_log partition(dt='$to_date');
"

$hive -e "$sql"
