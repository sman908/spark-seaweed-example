#! /usr/bin/env sh

ARG=$1
SPARK_PATH=${ARG:-/Users/m/code/spark-play/spark-3.1.2-bin-hadoop2.7}

"$SPARK_PATH/bin/spark-shell" \
    --conf spark.hadoop.fs.seaweedfs.impl=seaweed.hdfs.SeaweedFileSystem \
    --conf spark.hadoop.fs.defaultFS=seaweedfs://seaweedfs:8888 \
    --conf spark.driver.extraClassPath="$SPARK_PATH/seaweedfs-hadoop2-client-1.6.9.jar" \
    --master spark://localhost:7077
