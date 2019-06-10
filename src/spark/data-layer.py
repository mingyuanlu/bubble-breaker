import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from entity_codes import country_names, category_names
from configs import cassandra_cluster_ips
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from cassandra.cluster import Cluster
import pyspark_cassandra
from enum import Enum
cluster = Cluster(cassandra_cluster_ips)

def country_exists(r):
   if r in country_names:
       return r
   return ''

def event_exists(r):
   if r in category_names:
       return r
   return ''

country_exists = udf(country_exists,StringType())
category_exists = udf(event_exists,StringType())

# Set Spark configurations
config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.aws/credentials'))
access_id = config.get('default', "aws_access_key_id")
access_key = config.get('default', "aws_secret_access_key")
spark = SparkSession.builder \
    .appName("hawk-eye") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()

# Set HDFS configurations
sc=spark.sparkContext
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

dataRDD = sc.textFile('s3a://gdelt-open-data/events/201[4-8]*')
dataRDD = dataRDD.map(lambda x: x.encode("utf", "ignore"))
dataRDD.cache()
dataRDD  = dataRDD.map(lambda x : x.split('\t'))
rowRDD = dataRDD.map(lambda x : Row(date = x[1],
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
sqlContext = SQLContext(sc)

schemaDF = sqlContext.createDataFrame(rowRDD)

sqlContext.registerDataFrameAsTable(schemaDF, 'temp')

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

def getTopTenEvents (t):
    country,date = t[0]
    listCategoryMentionsURL, listCategoryScoreMap = t[1]
    categoryTopEventsMap = {}
    urlCategoryToEventMap = {}
    # each event stored as (event_id, mentions, url)
    for categoryMention in listCategoryMentionsURL:
        category, numMentions, url, eventID = categoryMention
        if category not in categoryTopEventsMap:
            categoryTopEventsMap[category] = []
        # combine events with same SOURCEURL values
        if (url, category) in urlCategoryToEventMap:
            event_map = urlCategoryToEventMap[(url, category)]
            event_map['numMentions'] += numMentions
        else:
            event_map = {'eventID': eventID, 'numMentions': numMentions, 'url': url}
            categoryTopEventsMap[category].append(event_map)
            urlCategoryToEventMap[(url, category)] = event_map
        categoryTopEventsMap[category] = sorted(categoryTopEventsMap[category], key=lambda event_map: event_map['numMentions'], reverse=True)
        if (len(categoryTopEventsMap[category]) > 10):
            categoryTopEventsMap[category].pop()
    return (t[0], t[1], categoryTopEventsMap)

def aggregateScores (t):
    country,date = t[0]
    listCategoryMentionsURL, listCategoryScoreMap = t[1]
    avgCategoryScoreMap = {}
    countCategoryMap = {}
    for categoryScoreMap in listCategoryScoreMap:
        category, scoreMap = categoryScoreMap
        if category not in countCategoryMap:
            countCategoryMap[category] = 0
        countCategoryMap[category] += 1
        if category not in avgCategoryScoreMap:
            avgCategoryScoreMap[category] = {}
            avgCategoryScoreMap[category]['gs'] = 0
            avgCategoryScoreMap[category]['tone'] = 0
        # calculate average of GoldsteinScale
        if scoreMap['gs']:
            avgCategoryScoreMap[category]['gs'] = ( \
                avgCategoryScoreMap[category]['gs'] * (countCategoryMap[category] - 1) + \
                scoreMap['gs'])/countCategoryMap[category]
        # calculate average of Tone
        if scoreMap['tone']:
            avgCategoryScoreMap[category]['tone'] = ( \
                avgCategoryScoreMap[category]['tone'] * (countCategoryMap[category] - 1) + \
                scoreMap['tone'])/countCategoryMap[category]
    return (t[0], t[1], t[2], avgCategoryScoreMap)

def prepareToInsert (t):
    return (t[0][0], t[0][1], t[3], t[2])

filteredDF =  filteredDF \
        .withColumn("country",country_exists(col("country"))) \
        .filter("country != ''") \
	    .withColumn("actor_type",category_exists(col("actor_type"))) \
        .filter("actor_type != ''")

class TimePeriod(Enum):
    YEAR = 1
    MONTH = 2
    DAY = 3

def processDataForTimePeriod(timePeriod):
    if timePeriod == TimePeriod.YEAR:
        time_period_column = "year"
        table_name = "yearly_test"
    elif timePeriod == TimePeriod.MONTH:
        time_period_column = "month"
        table_name = "monthly_test"
    elif timePeriod == TimePeriod.DAY:
        time_period_column = "date"
        table_name = "daily_test"
    else:
        return

    timePeriodCassandraRDD = filteredDF.rdd \
        .map(lambda a: ( \
            (a["country"], a[time_period_column]),
            (
                [(a["actor_type"], a["num_mentions"], a["mention_source"], a["event_id"])], # [("GOV", 30, "http://bbc/article123")]   [("CVL", 50, "http://cnn/article456")]
                [(a["actor_type"], {"gs": a["goldstein_scale"], "tone": a["tone"]})] \
            ) \
        )) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
        .map(getTopTenEvents) \
        .map(aggregateScores) \
        .map(prepareToInsert)

    timePeriodCassandraRDD.saveToCassandra("hawkeye", table_name)


processDataForTimePeriod(TimePeriod.YEAR)
processDataForTimePeriod(TimePeriod.MONTH)
processDataForTimePeriod(TimePeriod.DAY)

