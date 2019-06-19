from pyspark.sql import functions

import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import udf, col, explode, avg, count, max, min, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
from enum import Enum
import numpy as np


def transform_to_timestamptz(t):
    """
    Transform GDETL mention datetime to timestamp format 
    (YYYY-MM-DD HH:MM:SS)  for TimescaleDB
    """
    return t[:4]+'-'+t[4:6]+'-'+t[6:8]+' '+t[8:10]+':'+t[10:12]+':'+t[12:14]

def get_quantile(data):
    """
    Return the 0, 0.25, 0.5, 0.75, 1 quantiles of data
    """
    arr = np.array(data)
    q = np.array([0, 0.25, 0.5, 0.75, 1])
    #print np.quantile(arr, q)
    return np.quantile(arr, q).tolist()

    

def hist_data(data):
    """
    Return number of entry in each bin for a histogram
    of range (-10, 10) with 10 bins. Bin 0 and 11 are 
    under/overflow bins
    """
    minVal=-10
    maxVal=10
    nBins=10
    bins = [0]*(nBins+2)
    step = (maxVal - minVal) / float(nBins)
    for d in data:
        if d<minVal:
            bins[0] += 1
        elif d>maxVal:
            bins[nBins+1] += 1
        else:
            for b in range(1, nBins+1):
                if d < minVal+float(b)*step:
                    bins[b] += 1
                    break

    return bins


def main(sc):
    """
    Read GDELT data from S3, select columns, join tables,
    and perform calculations with grouped themes and document
    times
    """

    #Read "mentions" table from GDELT S3 bucket. Transform into RDD
    mentionRDD = sc.textFile('s3a://gdelt-open-data/v2/mentions/201807200000*.mentions.csv')
    mentionRDD = mentionRDD.map(lambda x: x.encode("utf", "ignore"))
    mentionRDD.cache()
    mentionRDD  = mentionRDD.map(lambda x : x.split('\t'))
    mentionRowRDD = mentionRDD.map(lambda x : Row(event_id = x[0],
                                        mention_id = x[5],
                                        mention_doc_tone = float(x[13]),
                                        mention_time_date = transform_to_timestamptz(x[2]),
                                        event_time_date = x[1],
                                        mention_src_name = x[4]))

    
    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/201807200000*.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRowRDD = gkgRDD.map(lambda x : Row(src_common_name = x[3],
                                        doc_id = x[4],
                                        themes = x[7].split(';')[:-1]
                                        ))



    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    mentionDF = sqlContext.createDataFrame(mentionRowRDD)
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    sqlContext.registerDataFrameAsTable(mentionDF, 'temp1')
    sqlContext.registerDataFrameAsTable(gkgDF, 'temp2')


    df1 = mentionDF.alias('df1')
    df2 = gkgDF.alias('df2')

    #Themes and tones information are stored in two different tables
    joinedDF = df1.join(df2, df1.mention_id == df2.doc_id, "inner").select('df1.*'
                                                , 'df2.src_common_name','df2.themes')
    #joinedDF.show()

    #Each document could contain multiple themes. Explode on the themes and make a new column
    explodedDF = joinedDF.select('event_id', 'mention_id', 'mention_doc_tone'
                                                , 'mention_time_date', 'event_time_date'
                                                , 'mention_src_name', 'src_common_name'
                                                , explode(joinedDF.themes).alias("theme"))

    
    #explodedDF.registerTempTable('df3')
    #quantilesDF = sqlContext.sql("""SELECT
    #                            COUNT(mention_doc_tone)                   AS num_mentions,
    #                            AVG(mention_doc_tone)                     AS avg,
    #                            MIN(mention_doc_tone)                     AS quantile_0,
    #                            percentile(mention_doc_tone, 0.25) AS quantile_25,
    #                            percentile(mention_doc_tone, 0.5)  AS quantile_50,
    #                            percentile(mention_doc_tone, 0.75) AS quantile_75,
    #                            MAX(mention_doc_tone)                     AS quantile_100
    #                            FROM df3 GROUP BY theme, mention_time_date""")


    #quantilesDF.show()

    hist_data_udf = udf(hist_data, ArrayType(IntegerType()))
    get_quantile_udf = udf(get_quantile, ArrayType(FloatType()))

    #Compute statistics for each theme at a time
    testDF = explodedDF.groupBy('theme', 'mention_time_date').agg(
            count('*').alias('num_mentions'),
            avg('mention_doc_tone').alias('avg'),
            collect_list('mention_doc_tone').alias('tones') 
            )
    #testDF.show()

    #Histogram and compute  quantiles for tones
    histDF = testDF.withColumn("bin_vals", hist_data_udf('tones')) \
                   .withColumn("quantiles", get_quantile_udf('tones'))

    histDF.drop('tones')
    #histDF.show()
    finalDF = histDF.select('theme', 'num_mentions', 'avg', 'quantiles', 'bin_vals', col('mention_time_date').alias('time'))
    finalDF.show()
    


    #sampleData = [('test_theme',5,0,[-2,-1,1,2],[0,1,1,2,1,0,6,0],'2016-06-22 19:10:57')]
    #testDF = sqlContext.createDataFrame(sampleData, schema=["theme","num_mentions","avg","quantiles","bin_vals","time"])
    #testDF.show()

    #Preparing to write to TimescaleDB
    db_properties = {}
    config = configparser.ConfigParser()
    config.read("db_properties.ini")
    db_prop = config['postgresql']
    db_url = db_prop['url']
    db_properties['username'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']

    #Write to table 
    finalDF.write.format("jdbc").options(
    url=db_properties['url'],
    dbtable='bubblebreaker_schema.tones_table',
    user='postgres',
    password='postgres',
    stringtype="unspecified"
    ).mode('append').save()
   

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

    main(sc)

