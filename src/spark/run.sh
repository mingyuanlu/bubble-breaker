# !/bin/bash
spark-submit \
  --executor-memory 5G \
  --driver-memory 6G \
  --master spark://ip-10-0-0-13:7077 \
  --num-executors 4 \
 --conf spark.executor.memoryOverhead=600 \
  --total-executor-cores 8 \
  --py-files functions.py \
  $1
