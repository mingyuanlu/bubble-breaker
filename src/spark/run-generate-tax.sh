#!/bin/bash
spark-submit \
   --master spark://ip-10-0-0-13:7077 \
   --executor-memory 6G \
   --driver-memory 6G \
   --num-executors 4 \
   --executor-cores 1 \
   --conf spark.executor.memoryOverhead=1024 \
   --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
   --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
   --py-files functions.py \
   --total-executor-cores 4 \
   $1 $2 $3
