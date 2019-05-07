#! /bin/bash

case $1 in
"start"){
        echo " -------- 启动 KafkaManager -------"
        nohup /opt/module/kafka-manager-1.3.3.22/bin/kafka-manager   -Dhttp.port=7456 >start.log 2>&1 &
};;
"stop"){
        echo " -------- 停止 KafkaManager -------"
        ssh hadoop102 "ps -ef | grep ProdServerStart | grep -v grep |awk '{print \$2}' | xargs kill -9"
};;
esac
