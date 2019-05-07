#!/bin/bash
PROJECT=gmall1111
JAVA_BIN=/opt/module/jdk1.8.0_144/bin/java
APPNAME=dw-logger-0.0.1-SNAPSHOT.jar
SERVER_PORT=8080
 
case $1 in
 "start")
   {
    for i in hadoop102 hadoop103 hadoop104
    do
     echo "========启动日志服务: $i==============="
    ssh $i  "$JAVA_BIN -Xms32m -Xmx64m  -jar /applog/$PROJECT/$APPNAME --server.port=$SERVER_PORT >/dev/null 2>&1  &"
    done

    echo "========启动NGINX==============="
    /opt/module/nginx/sbin/nginx

  };;
  "stop")
  {
     echo "========关闭 NGINX==============="
    /opt/module/nginx/sbin/nginx  -s stop

    for i in hadoop102 hadoop103 hadoop104
    do
     echo "========关闭日志服务: $i==============="
     ssh $i "ps -ef|grep $APPNAME |grep -v grep|awk '{print \$2}'|xargs kill" >/dev/null 2>&1
    done
 
  };;
   esac
