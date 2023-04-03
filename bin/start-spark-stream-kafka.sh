#!/bin/bash
/data/spark-3.2.3-bin-hadoop3.2/bin/spark-submit \
 --master spark://10.0.65.157:7077,10.0.65.158:7077 \
  --deploy-mode client \
   --executor-memory 1G \
   --total-executor-cores 1 \
    --driver-memory 1G \
      --class com.msb.App \
       /data/spark-job/msb-kafka-spark-stream-1.0-jar-with-dependencies.jar \
        "process_json_in" \
        "/data/spark-job/config/application.properties" \
        "/data/spark-job/config/redis-config.xml"