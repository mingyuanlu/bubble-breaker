#from pyspark.sql import SparkSession
#from pyspark.sql import Row
from pyspark.sql import functions

import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
#from entity_codes import country_names, category_names
#from configs import cassandra_cluster_ips
from pyspark.sql.functions import udf, col, explode, avg
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql.types import StringType
#from cassandra.cluster import Cluster
#import pyspark_cassandra
from enum import Enum
#from cassandra.cluster import Cluster
#import pyspark_cassandra
import psycopg2

#try:
#
#    connection = psycopg2.connect(host='ec2-3-215-225-40.compute-1.amazonaws.com',
#                    user = 'postgres',
#                    password = 'postgres'
#                    )
#    cursor = connection.cursor()
#except:
#    print("Error connecting to database!")
#    exit(1)


#cassandra_cluster_ips = ["54.211.70.104"]
#cluster = Cluster(cassandra_cluster_ips)


# Set Spark configurations

#config = configparser.ConfigParser()
#config.read(os.path.expanduser('~/.aws/credentials'))
#access_id = config.get('default', "aws_access_key_id")
#access_key = config.get('default', "aws_secret_access_key")
#spark = SparkSession.builder \
#    .appName("buuble-breaker") \
#    .config("spark.executor.memory", "1gb") \
#    .getOrCreate()
#
## Set HDFS configurations
#sc=spark.sparkContext
#
#hadoop_conf=sc._jsc.hadoopConfiguration()
#hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#hadoop_conf.set("fs.s3a.access.key", access_id)
#hadoop_conf.set("fs.s3a.secret.key", access_key)
#
##hadoop_conf.set("fs.s3a.access.key", access_id)
##hadoop_conf.set("fs.s3a.secret.key", access_key)
#
##hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
##hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
##hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)
#
##hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
##hadoop_conf.set("fs.s3.awsAccessKeyId", access_id)
##hadoop_conf.set("fs.s3.awsSecretAccessKey", access_key)
#
#'''
#def loadTopicNames(listOfFiles):
#    topicNames = {}
#    for ff in listOfFiles:
#        with open(ff, encoding="ISO-8859-1") as f:
#            for line in f:
#                fields = line.split('\t')
#                movieNames[int(fields[0])] = fields[1]
#    return movieNames
#'''
#
#
#
#sc=spark.sparkContext
#
def main(sc):

#dataRDD = sc.textFile('s3n://gdelt-open-data/events/201[4-8]*')
    mentionRDD = sc.textFile('s3a://gdelt-open-data/v2/mentions/201807200000*.mentions.csv')
    #mentionRDD = sc.textFile(sys.argv[1])
    mentionRDD = mentionRDD.map(lambda x: x.encode("utf", "ignore"))
    mentionRDD.cache()
    mentionRDD  = mentionRDD.map(lambda x : x.split('\t'))
    mentionRowRDD = mentionRDD.map(lambda x : Row(event_id = x[0],
                                        mention_id = x[5],
                                        mention_doc_tone = x[13],
                                        mention_time_date = x[2],
                                        event_time_date = x[1],
                                        mention_src_name = x[4]))

    gkgRDD = sc.textFile('s3a://gdelt-open-data/v2/gkg/201807200000*.gkg.csv')
    #gkgRDD = sc.textFile(sys.argv[2])
    gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
    gkgRDD.cache()
    gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
    gkgRowRDD = gkgRDD.map(lambda x : Row(src_common_name = x[3],
                                        doc_id = x[4],
                                        #themes = map(lambda s: s.split(';')[:-1], x[7])
                                        #themes = x[7].map(lambda x: x.split(';')[:-1])
                                        themes = x[7].split(';')[:-1]
                                        ))



    '''
    rowRDD = mentionRDD.map(lambda x : Row(date = x[1],
                                        month = x[2],
                                        year = x[3],
                                        country = x[51],
                                        actortype1 = x[12],
                                        actortype2 = x[13],
                                        goldstein_scale = x[30],
                                        num_mentions = x[31],
                                        tone = x[34],
                                        event_id = x[0],
                                        mention_source = x[57]))
    '''
    sqlContext = SQLContext(sc)

    #schemaDF = sqlContext.createDataFrame(rowRDD)
    mentionDF = sqlContext.createDataFrame(mentionRowRDD)
    gkgDF     = sqlContext.createDataFrame(gkgRowRDD)

    #sqlContext.registerDataFrameAsTable(schemaDF, 'temp')
    sqlContext.registerDataFrameAsTable(mentionDF, 'temp1')
    sqlContext.registerDataFrameAsTable(gkgDF, 'temp2')

    count = mentionDF.groupBy('event_id').count().cache()
    top10 = count.take(10)
    for result in top10:
         print("%s: %d") % (result[0], result[1])

    df1 = mentionDF.alias('df1')
    df2 = gkgDF.alias('df2')

    joinedDF = df1.join(df2, df1.mention_id == df2.doc_id, "inner").select('df1.*', 'df2.src_common_name','df2.themes')

    #joinedDF = mentionDF.join(gkgDF, mentionDF("mention_id") == gkgDF("doc_id"), "inner") #.select("code", "date")
    joinedDF.show()

    theme_array = [row.themes for row in joinedDF.collect()]
    #print theme_array
    #theme_array = []
    #joinedDF.select(explode(joinedDF.themes.split(';')[:-1]).alias("theme")).collect()
    #joinedDF = joinedDF.select()
    #joinedDF.select('event_id', 'mention_doc_tone', explode(joinedDF.themes).alias("theme")).show()
    explodedDF = joinedDF.select('event_id', 'mention_id', 'mention_doc_tone', 'mention_time_date', 'event_time_date', 'mention_src_name', 'src_common_name', explode(joinedDF.themes).alias("theme"))



    #themeDF = explodedDF.groupBy('theme')
    #themeDF.show()

    #resultDF = themeDF.select('theme', themeDF.count().alias('num_mentions'), #themeDF.agg(avg(col('mention_doc_tone'))).alias('avg'),
    #themeDF.agg(statFunc.approxQuantile("mention_doc_tone"))
    #
    sampleData = [('test_theme',5,0,[-2,-1,1,2],[0,1,1,2,1,0,6,0],'2016-06-22 19:10:57')]
    testDF = sqlContext.createDataFrame(sampleData, schema=["theme","num_mentions","avg","quantiles","bin_vals","time"])
    testDF.show()



    #Assume the exploded DF is explodedDF
    sqlContext.registerDataFrameAsTable(explodedDF, 'temp3')

    #themeDF = sqlContext.sql("""SELECT AVG(mention_doc_tone),
    #                                    mention_doc_tone.count(),




     #FROM temp3 GROUP BY theme""")
    #med = themeDF.approxQuantile("mention_doc_tone", [0.5], 0.25)
    #print("type: %s") % (type(med))
    #med.show()


    #Get average of tone for each theme
    '''
    avgToneDF = sqlContext.sql("""SELECT mention_id,
                                CAST(event_id AS INTEGER),
                                mention_time_date,
                                event_time_date,
                                mention_src_name,
                                src_common_name,
                                AVG(mention_doc_tone)
                                FROM temp3
                                GROUP BY theme
                                """)
    '''
    '''
    avgToneDF = sqlContext.sql("""SELECT
                                theme,
                                AVG(mention_doc_tone) WITHIO,
                                percentile_desc

                                FROM temp3
                                GROUP BY theme
                                """)


    avgToneDF.show()
    table_name = "test"
    avgTone = avgToneDF.rdd.map(list)
    '''
    #avgTone.saveToCassandra("bubble-breaker", table_name)
    #first10 = explodedDF.take(10)
    #for t in first10:
    #    print t

    db_properties = {}
    config = configparser.ConfigParser()
    config.read("db_properties.ini")
    db_prop = config['postgresql']
    for k in db_prop:
        print db_prop[k]
    db_url = db_prop['url']
    db_properties['username'] = db_prop['username']
    db_properties['password'] = db_prop['password']
    db_properties['url'] = db_prop['url']
    db_properties['driver'] = db_prop['driver']

    #testDF.write.jdbc(url=db_url, table='bubblebreaker_schema.tones_table',mode='overwrite',properties=db_properties)

    testDF.write.format("jdbc").options(
    url=db_properties['url'],
    dbtable='bubblebreaker_schema.tones_table',
    user='postgres',
    password='postgres',
    stringtype="unspecified"
    ).mode('append').save()

    '''

    #Count the number of
    filteredDF = sqlContext.sql("""SELECT CAST(date AS INTEGER),
                                   CAST(month AS INTEGER),
                                   CAST(year AS INTEGER),
                                   country,
                                   CASE WHEN actortype1 = '' AND actortype2 <> '' THEN actortype2
    				               ELSE actortype1 END AS actor_type,
                                   CAST(goldstein_scale AS INTEGER),
                                   CAST(num_mentions AS INTEGER),
                                   CAST(tone AS INTEGER),
                                   mention_source,
                                   event_id
                              FROM temp
                             WHERE country <> '' AND country IS NOT NULL
                                AND (actortype1 <> '' OR actortype2 <> '')
                                AND goldstein_scale <> '' AND goldstein_scale IS NOT NULL
                                AND mention_source <> '' AND mention_source IS NOT NULL""")
    '''

if __name__ == '__main__':
    """
    Setting up Spark session and Spark context, AWS access key
    """
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('default', "aws_access_key_id")
    access_key = config.get('default', "aws_secret_access_key")
    spark = SparkSession.builder \
        .appName("buuble-breaker") \
        .config("spark.executor.memory", "1gb") \
        .getOrCreate()

    sc=spark.sparkContext

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    main(sc)
