# !/bin/bash
spark-submit \
  --master spark://ec2-**-**-**-***.us-west-2.compute.amazonaws.com:7077 \
  --executor-memory 8G \
  --driver-memory 8G \
  --packages anguenot/pyspark-cassandra:0.9.0,com.databricks:spark-csv_2.10:1.2.0 \
  --conf spark.cassandra.connection.host=ip1.***.***.**,ip2.***.***.**,ip3.***.***.** \
  --conf spark.driver.port=51810 \
  --conf spark.fileserver.port=51811 \
  --conf spark.broadcast.port=51812 \
  --conf spark.replClassServer.port=51813 \
  --conf spark.blockManager.port=51814 \
  --conf spark.executor.port=51815 \
  data-layer.py

