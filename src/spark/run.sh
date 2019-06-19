# !/bin/bash
spark-submit \
  --executor-memory 1G \
  --driver-memory 6G \
  --master spark://ip-10-0-0-8:7077 \
  --num-executors 3
  compute-avg-tone-per-theme.py
