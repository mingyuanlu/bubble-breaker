from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, IntegerType
# from abbreviations_dict import tofullname, toevent
from pyspark.sql import SparkSession
from operator import itemgetter
from pyspark import StorageLevel
import pyspark_cassandra
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.aws/credentials'))
access_id = config.get('default', "aws_access_key_id")
access_key = config.get('default', "aws_secret_access_key")
spark = SparkSession.builder   .appName("hawk-eye")    .config("spark.executor.memory", "1gb")    .getOrCreate()
sc=spark.sparkContext
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)
sqlContext = SQLContext(sc)

customSchema =  StructType([
        StructField('GLOBALEVENTID',StringType(),True),
        StructField('SQLDATE',StringType(),True),
        StructField('MonthYear',StringType(),True),
        StructField('Year',StringType(),True),
        StructField('FractionDate',StringType(),True),
        StructField('Actor1Code',StringType(),True),
        StructField('Actor1Name',StringType(),True),
        StructField('Actor1CountryCode',StringType(),True),
        StructField('Actor1KnownGroupCode',StringType(),True),
        StructField('Actor1EthnicCode',StringType(),True),
        StructField('Actor1Religion1Code',StringType(),True),
        StructField('Actor1Religion2Code',StringType(),True),
        StructField('Actor1Type1Code',StringType(),True),
        StructField('Actor1Type2Code',StringType(),True),
        StructField('Actor1Type3Code',StringType(),True),
        StructField('Actor2Code',StringType(),True),
        StructField('Actor2Name',StringType(),True),
        StructField('Actor2CountryCode',StringType(),True),
        StructField('Actor2KnownGroupCode',StringType(),True),
        StructField('Actor2EthnicCode',StringType(),True),
        StructField('Actor2Religion1Code',StringType(),True),
        StructField('Actor2Religion2Code',StringType(),True),
        StructField('Actor2Type1Code',StringType(),True),
        StructField('Actor2Type2Code',StringType(),True),
        StructField('Actor2Type3Code',StringType(),True),
        StructField('IsRootEvent',StringType(),True),
        StructField('EventCode',StringType(),True),
        StructField('EventBaseCode',StringType(),True),
        StructField('EventRootCode',StringType(),True),
        StructField('QuadClass',StringType(),True),
        StructField('GoldsteinScale',StringType(),True),
        StructField('NumMentions',StringType(),True),
        StructField('NumSources',StringType(),True),
        StructField('NumArticles',StringType(),True),
        StructField('AvgTone',StringType(),True),
        StructField('Actor1Geo_Type',StringType(),True),
        StructField('Actor1Geo_FullName',StringType(),True),
        StructField('Actor1Geo_CountryCode',StringType(),True),
        StructField('Actor1Geo_ADM1Code',StringType(),True),
        StructField('Actor1Geo_Lat',StringType(),True),
        StructField('Actor1Geo_Long',StringType(),True),
        StructField('Actor1Geo_FeatureID',StringType(),True),
        StructField('Actor2Geo_Type',StringType(),True),
        StructField('Actor2Geo_FullName',StringType(),True),
        StructField('Actor2Geo_CountryCode',StringType(),True),
        StructField('Actor2Geo_ADM1Code',StringType(),True),
        StructField('Actor2Geo_Lat',StringType(),True),
        StructField('Actor2Geo_Long',StringType(),True),
        StructField('Actor2Geo_FeatureID',StringType(),True),
        StructField('ActionGeo_Type',StringType(),True),
        StructField('ActionGeo_FullName',StringType(),True),
        StructField('ActionGeo_CountryCode',StringType(),True),
        StructField('ActionGeo_ADM1Code',StringType(),True),
        StructField('ActionGeo_Lat',StringType(),True),
        StructField('ActionGeo_Long',StringType(),True),
        StructField('ActionGeo_FeatureID',StringType(),True),
        StructField('DATEADDED',StringType(),True),
        StructField('SOURCEURL',StringType(),True)])

df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false') \
    .options(delimiter="\t") \
    .load('s3a://gdelt-open-data/events/*', schema = customSchema)

base = 's3n://rp-hawk-eye-data/parquet/'
df.write.parquet(base)

