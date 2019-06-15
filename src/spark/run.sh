# !/bin/bash
spark-submit \
  --executor-memory 1G \
  --driver-memory 8G \
  --packages anguenot/pyspark-cassandra:0.9.0,com.databricks:spark-csv_2.10:1.2.0 \
  --conf spark.cassandra.connection.host=172.31.90.30 \
  --conf spark.driver.port=51810 \
  --conf spark.fileserver.port=51811 \
  --conf spark.broadcast.port=51812 \
  --conf spark.replClassServer.port=51813 \
  --conf spark.blockManager.port=51814 \
  --conf spark.executor.port=51815 \
  compute-avg-tone-per-theme.py
