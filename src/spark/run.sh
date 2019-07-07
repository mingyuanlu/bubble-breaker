# !/bin/bash
spark-submit \
  --executor-memory 3G \
  --driver-memory 6G \
  --master spark://ip-10-0-0-13:7077 \
  --num-executors 7 \
  --executor-cores 5 \
  --conf spark.executor.memoryOverhead=600 \
  --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  --py-files functions.py \
  --total-executor-cores 35 \
  $1
