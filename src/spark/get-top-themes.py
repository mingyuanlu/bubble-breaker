import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from pyspark.sql.functions import rank, udf, col, explode, avg, count, max, min, collect_list
from pyspark.sql.types import StringType, ArrayType, FloatType, IntegerType
from pyspark.sql.window import Window
import numpy as np
import functions as f


def main(sc, out_file_name):
    """
    Read GDELT data from S3, clean themes from taxonomy words, and 
    perform frequency count of cleaned themes. Pick top 1000 most
    popular themes and write to out_file_name
    """

    #Obtain list of taxonomy words for theme cleaning
    tax_file = os.environ['TAX_LIST_FILE']
    tax_list = f.read_tax_file(tax_file)
    rdd_tax_list = sc.broadcast(tax_list)


    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD and clean taxonomy words
    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/201[5-9]*000000.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRDD = gkgRDD.filter(lambda x: len(x)==27)   
    gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty(x[7]]))
    gkgRowRDD = gkgRDD.map(lambda x : Row(themes = f.clean_taxonomy(x[7].split(';')[:-1], rdd_tax_list)))


    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    #Each document could contain multiple themes. Explode on the themes and make a new column
    explodedDF = gkgDF.select(explode(gkgDF.themes).alias("theme"))

    #Count the frequency of each theme
    testDF = explodedDF.groupBy('theme').agg(count('*').alias('num_mentions'))

    #Find top 1000 most popular themes, use Pandas to write to output file
    window = Window.orderBy(testDF['num_mentions'].desc())
    rankDF = testDF.select('*', rank().over(window).alias('rank')) .filter(col('rank') <= 1000).where(col('theme') != '')
    pandasDF = rankDF.toPandas()
    pandasDF.to_csv(out_file_name, columns = ["theme", "num_mentions", "rank"])

    

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

    out_file_name = sys.argv[1]

    main(sc, out_file_name)
