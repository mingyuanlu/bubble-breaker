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

def has_no_numbers(inputString):
    return not(any(char.isdigit() for char in inputString))

def pick_first_two(l):
    return l[:2]

def get_len(l):
    return len(l)

def main(sc, occurrence_cut, out_file_name):
    """
    Read GDELT data from S3, select columns, join tables,
    and perform calculations with grouped themes and document
    times
    """

    #Read "mentions" table from GDELT S3 bucket. Transform into RDD
    #mentionRDD = sc.textFile('s3a://gdelt-open-data/v2/mentions/201807200000*.mentions.csv')
    #mentionRDD = mentionRDD.map(lambda x: x.encode("utf", "ignore"))
    #mentionRDD.cache()
    #mentionRDD  = mentionRDD.map(lambda x : x.split('\t'))
    #mentionRowRDD = mentionRDD.map(lambda x : Row(event_id = x[0],
    #                                    mention_id = x[5],
    #                                    mention_doc_tone = float(x[13]),
    #                                    mention_time_date = transform_to_timestamptz(x[2]),
    #                                    event_time_date = x[1],
    #                                    mention_src_name = x[4]))


    #Read 'GKG" table from GDELT S3 bucket. Transform into RDD
    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/2018*.gkg.csv')
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRDD = gkgRDD.filter(lambda x: len(x)==27)
    gkgRDD = gkgRDD.filter(lambda x: f.is_not_empty([x[7]]))
    gkgRowRDD = gkgRDD.map(lambda x : Row(
                                        #src_common_name = x[3],
                                        #doc_id = x[4],
                                        themes = x[7].split(';')[:-1]
                                        ))
                      #.map(lambda x: x.split('_')[:1])



    sqlContext = SQLContext(sc)

    #Transform RDDs to dataframes
    #mentionDF = sqlContext.createDataFrame(mentionRowRDD)
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    #sqlContext.registerDataFrameAsTable(mentionDF, 'temp1')
    #sqlContext.registerDataFrameAsTable(gkgDF, 'temp2')


    #df1 = mentionDF.alias('df1')
    #df2 = gkgDF.alias('df2')

    #Themes and tones information are stored in two different tables
    #joinedDF = df1.join(df2, df1.mention_id == df2.doc_id, "inner").select('df1.*'
    #                                            , 'df2.src_common_name','df2.themes')

    #Each document could contain multiple themes. Explode on the themes and make a new column
    #explodedDF = joinedDF.select('event_id', 'mention_id', 'mention_doc_tone'
    #                                            , 'mention_time_date', 'event_time_date'
    #                                            , 'mention_src_name', 'src_common_name'
    #                                            , explode(joinedDF.themes).alias("theme"))

    explodedDF = gkgDF.select(#'event_id', 'mention_id', 'mention_doc_tone'
                                #                , 'mention_time_date', 'event_time_date'
                                #                , 'mention_src_name', 'src_common_name'
                                                explode(gkgDF.themes).alias("theme")) \
                      .distinct()

    clean_comma_list_udf = udf(f.clean_comma_list, ArrayType(StringType()))
    has_no_numbers_udf   = udf(has_no_numbers, BooleanType())
    pick_first_two_udf   = udf(pick_first_two, ArrayType(StringType()))
    get_len_udf          = udf(get_len, IntegerType())

    #filteredDF = explodedDF.select(explode(split(col('theme'),'_')[0:2].alias('theme_element'))
                           #.filter(has_no_numbers_udf("theme_element"))
    #                       )
    #explodedDF.select(split(col('theme'),'_').alias('theme_element'))
    tempDF = explodedDF.withColumn('theme_element_with_comma', split(col('theme'),'_')) \
                       .withColumn('theme_element', clean_comma_list_udf('theme_element_with_comma')) \
                       .filter(get_len_udf('theme_element')>1) \
                       .withColumn('first_two', pick_first_two_udf('theme_element')) \
                       .select(explode('first_two').alias('first_two_themes')) \
                       .filter(has_no_numbers_udf('first_two_themes')) \
                       .groupBy('first_two_themes') \
                       .count()
    #tempDF.show()
    pandasDF = tempDF.toPandas()
    taxDF = pandasDF[pandasDF["count"] >= occurrence_cut]
    #print(taxDF)
    taxDF.to_csv(out_file_name, columns = ["first_two_themes", "count"])
    #tempDF.write.csv('tax.csv')
    #ax = plt.subplot(1,1,1)
    #ax.set_yscale('log')
    #ax.set_ylim(5e-1, 1e3)
    #ax.set_xlim(-10,50)
    #ax.set_xlim(-50,2000)
    #ax.set_xlabel('Occurrence')
    #histogram = tempDF.select('count').rdd.flatMap(lambda x: x).histogram(100)
    #pd.DataFrame(list(zip(*histogram)), columns=['Occurrence', 'Word Entry']).set_index('Occurrence').plot(kind='bar');
    #ax.set_yscale('log')
    #pandasDF.hist(column='count', ax=ax, bins=10001)
    #plt.tight_layout()
    #plt.savefig('tax.png', bbox_inches='tight')
    #ax.set_xlim(-50,200)
    #plt.savefig('tax_zoom_2018.png', bbox_inches='tight')
    #countDF = filteredDF.groupBy('theme_element').count()
    #countDF.show()
#    hist_data_udf = udf(hist_data, ArrayType(IntegerType()))
#    get_quantile_udf = udf(get_quantile, ArrayType(FloatType()))
#
#
#    #Compute statistics for each theme at a time
#    testDF = explodedDF.groupBy('theme', 'mention_time_date').agg(
#            count('*').alias('num_mentions'),
#            avg('mention_doc_tone').alias('avg'),
#            collect_list('mention_doc_tone').alias('tones')
#            )
#
#    #Histogram and compute  quantiles for tones
#    histDF = testDF.withColumn("bin_vals", hist_data_udf('tones')) \
#                   .withColumn("quantiles", get_quantile_udf('tones'))
#
#    histDF.drop('tones')
#    #histDF.show()
#    finalDF = histDF.select('theme', 'num_mentions', 'avg', 'quantiles', 'bin_vals', col('mention_time_date').alias('time'))
#    finalDF.show()
#
#
#    #Preparing to write to TimescaleDB
#    db_properties = {}
#    config = configparser.ConfigParser()
#    config.read("db_properties.ini")
#    db_prop = config['postgresql']
#    db_url = db_prop['url']
#    db_properties['username'] = db_prop['username']
#    db_properties['password'] = db_prop['password']
#    db_properties['url'] = db_prop['url']
#    db_properties['driver'] = db_prop['driver']
#
#    #Write to table
#    finalDF.write.format("jdbc").options(
#    url=db_properties['url'],
#    dbtable='bubblebreaker_schema.tones_table',
#    user='postgres',
#    password='postgres',
#    stringtype="unspecified"
#    ).mode('append').save()


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
