import matplotlib
matplotlib.use('Agg')
import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list, split, rank
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType, BooleanType, DataType
from pyspark.sql.window import Window
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import functions as f

def main(sc, out_file_name):
    """
    Read GDELT data from S3, count occurrence of news sources,
    determine the top 100 most frequent ones, and write list to
    out_file_name
    """

    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/2018*.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRDD = gkgRDD.filter(lambda x: len(x)==27)
    gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty([x[3]]))
    gkgRowRDD = gkgRDD.map(lambda x : Row(src_common_name = x[3]))

    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    #Frequency count for each source
    srcDF = gkgDF.select('src_common_name').groupBy('src_common_name').agg(count('*').alias('count'))

    #Select top 100 most frequent sources, and write to output file
    window = Window.orderBy(srcDF['count'].desc())
    rankDF = srcDF.select('*', rank().over(window).alias('rank')) .filter(col('rank') <= 100).where(col('src_common_name') != '')
    pandasDF = rankDF.toPandas()
    pandasDF.to_csv(out_file_name, columns = ["src_common_name", "count", "rank"])


if __name__ == '__main__':
    """
    Setting up Spark session and Spark context, AWS access key
    """

    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('default', "aws_access_key_id")
    access_key = config.get('default', "aws_secret_access_key")
    spark = SparkSession.builder \
        .appName("bubble-breaker") \
        .config("spark.executor.memory", "1gb") \
        .getOrCreate()

    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)

    if len(sys.argv) < 2:
        print('Error! No occurrence cut provided!')
        exit(1)

    out_file_name = sys.argv[1]

    main(sc, out_file_name)
