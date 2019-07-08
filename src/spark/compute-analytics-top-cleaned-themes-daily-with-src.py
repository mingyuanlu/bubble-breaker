import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import dayofmonth, udf, col, explode, avg, count, max, min, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
import numpy as np
import functions as f

def main(sc):
    """
    Read GDELT data from S3, select columns, join tables,
    and perform calculations with grouped themes and document
    times
    """

    #Obtain taxonomy dictionary and broadcast to the workers
    tax_file = os.environ['TAX_LIST_FILE']
    tax_list = f.read_tax_file(tax_file)
    rdd_tax_list = sc.broadcast(tax_list)

    #Obtain list of top 500 themes used for filtering
    theme_file = os.environ['THEME_LIST_FILE']
    theme_list = f.read_theme_file(theme_file)
    rdd_theme_list = sc.broadcast(theme_list)

    #Obtainb list of top new src used for filtering
    src_file = os.environ['SRC_LIST_FILE']
    src_list = f.read_src_file(src_file)
    rdd_src_list = sc.broadcast(src_list)


    #Read "mentions" table from GDELT S3 bucket. Transform into RDD
    mentionRDD = sc.textFile('s3a://gdelt-open-data/v2/mentions/*.mentions.csv')
    mentionRDD = mentionRDD.map(lambda x: x.encode("utf", "ignore"))
    mentionRDD = mentionRDD.map(lambda x : x.split('\t'))
    mentionRDD = mentionRDD.filter(lambda x: len(x)==16)
    mentionRDD = mentionRDD.filter(lambda x: f.is_not_empty([x[2], x[5], x[13]]))
    mentionRDD = mentionRDD.filter(lambda x: f.is_number(x[13])) 
    mentionRowRDD = mentionRDD.map(lambda x : Row(
                                        mention_id = x[5],
                                        mention_doc_tone = float(x[13]),
                                        mention_time_date = f.transform_to_timestamptz_daily(x[2])
					))
 
    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/YEARMONTH*0000.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRDD = gkgRDD.filter(lambda x: len(x)==27)   
    gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty([x[3], x[4], x[7]]))
    gkgRowRDD = gkgRDD.map(lambda x : Row(src_common_name = x[3],
                                        doc_id = x[4],
                                        themes = f.clean_taxonomy(x[7].split(';')[:-1], rdd_tax_list)
                                        ))


    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    mentionDF = sqlContext.createDataFrame(mentionRowRDD)
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)


    df1 = mentionDF.alias('df1')
    df2 = gkgDF.alias('df2')

    #Themes and tones information are stored in two different tables
    joinedDF = df1.join(df2, df1.mention_id == df2.doc_id, "inner").select('df1.*'
                                                , 'df2.src_common_name','df2.themes').repartition(2000)

    #Each document could contain multiple themes. Explode on the themes and make a new column on filtered themes
    explodedDF = joinedDF.select('mention_id'
				, 'mention_doc_tone'
                                , 'mention_time_date'
				, 'src_common_name'
                                , explode(joinedDF.themes).alias("theme")) \
                                .filter(col('theme').isin(*(rdd_theme_list.value)))

    hist_data_udf = udf(f.hist_data, ArrayType(IntegerType()))
    get_quantile_udf = udf(f.get_quantile, ArrayType(FloatType()))
    
    #Compute statistics for each theme at a time
    explodedDF.cache()
    
    #Over all sources
    testDF1 = explodedDF.groupBy('theme', 'mention_time_date').agg(
            count('*').alias('num_mentions'),
            avg('mention_doc_tone').alias('avg'),
            collect_list('mention_doc_tone').alias('tones')
            )
    
    #For each source 
    testDF2 = explodedDF.groupBy('theme', 'mention_time_date', 'src_common_name').agg(
            count('*').alias('num_mentions'),
            avg('mention_doc_tone').alias('avg'),
            collect_list('mention_doc_tone').alias('tones')
            ).repartition(2000)
    
    #Histogram and compute quantiles for tones
    
    histDF1 = testDF1.withColumn("bin_vals", hist_data_udf('tones')) \
                   .withColumn("quantiles", get_quantile_udf('tones'))
    
    histDF2 = testDF2.withColumn("bin_vals", hist_data_udf('tones')) \
                   .withColumn("quantiles", get_quantile_udf('tones'))
   
    
    finalDF1 = histDF1.select('theme', 'num_mentions', 'avg', 'quantiles', 'bin_vals', col('mention_time_date').alias('time'))
    #Filter sources
    finalDF2 = histDF2.select('theme', 'src_common_name', 'num_mentions', 'avg', 'quantiles', 'bin_vals', 
            col('mention_time_date').alias('time')).filter(col('src_common_name').isin(*(rdd_src_list.value)))
    
    
    
    #Preparing to write to TimescaleDB
    #Fist write to group-by-src table
    
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
    finalDF1.write.format("jdbc").options(
    url=db_properties['url'],
    dbtable='bubblebreaker_schema.tones_table_v3',
    user='postgres',
    password='postgres',
    stringtype="unspecified"
    ).mode('append').save()
    
    #Then write to per-src table

    config.read("db_properties_src.ini")
    db_prop = config['postgresql']
    db_url = db_prop['url']
    db_properties['username'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']

    #Write to table
    finalDF2.write.format("jdbc").options(
    url=db_properties['url'],
    dbtable='bubblebreaker_src_schema.tones_table_v2',
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
        .getOrCreate()

    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)

    main(sc)
