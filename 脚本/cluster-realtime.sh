#!/bin/bash

case $1 in
"start"){
	echo " -------- 启动 集群 -------"

	#启动 Zookeeper集群
	zk.sh start

	#启动 Kafka采集集群
    kafka-cluster.sh start

	sleep 4s;

    #启动logger服务器接收日志
    logger-cluster.sh start
    
    #启动redis服务
    echo "=========启动redis服务==========="
    redis-server /myredis/redis.conf
    
    
    #启动es集群和kibana
    echo "=========启动es集群和kibana==========="
    es-cluster.sh start
    /opt/module/kibana/bin/kibana &

};;
"stop"){
        echo " -------- 停止 集群 -------"

	#停止es集群和kibana
    echo "=========停止es集群和kibana==========="
    ps -ef | grep -v grep | grep kibana | xargs kill -9
    es-cluster.sh stop
    
    #关闭redis服务
    echo "=========关闭redis服务==========="
    ps -ef | grep -v grep | grep redis | xargs kill -9

	#停止logger服务器接收日志
    logger-cluster.sh stop

	#停止 Kafka采集集群
    kafka-cluster.sh stop

    	sleep 4s;

	#停止 Zookeeper集群
	zk.sh stop

};;
esac
