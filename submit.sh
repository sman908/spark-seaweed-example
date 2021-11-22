#! /usr/bin/env sh

/Users/m/code/spark-play/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
    --master spark://localhost:7077 \
    --conf spark.hadoop.fs.seaweedfs.impl=seaweed.hdfs.SeaweedFileSystem \
    --conf spark.hadoop.fs.defaultFS=seaweedfs://seaweedfs:8888 \
    --deploy-mode cluster \
    --jars '/Users/m/code/spark-seaweed-example/target/pack/lib/*.jar' \
    --class Hello \
    ~/code/spark-seaweed-example/target/scala-2.12/sparkseaweedexample_2.12-0.1.jar
