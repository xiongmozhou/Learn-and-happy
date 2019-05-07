#!/bin/bash

for i in hadoop102 hadoop103
do
    ssh $i "java -classpath /opt/module/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain  >/opt/module/test.log &"
done
