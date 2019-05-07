#!/bin/bash


if [ "$1" == "" ];then
exit
fi


for i in hadoop102 hadoop103 hadoop104
do
    echo "================= $i =================="
    ssh $i "$*"
done

