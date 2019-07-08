import matplotlib
matplotlib.use('Agg')
import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list, split
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType, BooleanType, DataType
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import functions as f

def main(sc, occurrence_cut, out_file_name):
    """
    Read GDELT data from S3, split and process words in themes,
    and perform word count. Taxonomy words are defined as word
    count >= occurrence_cut. List of taxonomy words written to
    out_file_name
    """


    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
    gkgRDD = sc.textFile('s4a://gdelt-open-data/v2/gkg/2018*.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRDD = gkgRDD.filter(lambda x: len(x)==27)
    gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty([x[7]]))
    gkgRowRDD = gkgRDD.map(lambda x : Row(themes = x[7].split(';')[:-1]))


    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    #Explode so that each theme is a row
    explodedDF = gkgDF.select(explode(gkgDF.themes).alias("theme")).distinct()

    clean_comma_list_udf = udf(f.clean_comma_list, ArrayType(StringType()))
    has_no_numbers_udf   = udf(f.has_no_numbers, BooleanType())
    pick_first_two_udf   = udf(f.pick_first_two, ArrayType(StringType()))
    get_len_udf          = udf(f.get_len, IntegerType())

    #Split and process each word in a theme. Taxonomy words always appear within the first two words.
    #Reverse is not true
    tempDF = explodedDF.withColumn('theme_element_with_comma', split(col('theme'),'_')) \
                       .withColumn('theme_element', clean_comma_list_udf('theme_element_with_comma')) \
                       .filter(get_len_udf('theme_element')>1) \
                       .withColumn('first_two', pick_first_two_udf('theme_element')) \
                       .select(explode('first_two').alias('first_two_themes')) \
                       .filter(has_no_numbers_udf('first_two_themes')) \
                       .groupBy('first_two_themes') \
                       .count()

    #Write taxonomy words, defined as word count>=occurrence_cut, to output file using Pandas
    pandasDF = tempDF.toPandas()
    taxDF = pandasDF[pandasDF["count"] >= occurrence_cut]
    taxDF.to_csv(out_file_name, columns = ["first_two_themes", "count"])


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

    occurrence_cut = int(sys.argv[1])
    out_file_name = sys.argv[2]

    main(sc, occurrence_cut, out_file_name)
