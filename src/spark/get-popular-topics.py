#from pyspark.sql import SparkSession
#from pyspark.sql import Row
from pyspark.sql import functions

import sys
import os
from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext, Row
import configparser
from entity_codes import country_names, category_names
#from configs import cassandra_cluster_ips
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
#from cassandra.cluster import Cluster
#import pyspark_cassandra
from enum import Enum

'''
def loadTopicNames(listOfFiles):
    topicNames = {}
    for ff in listOfFiles:
        with open(ff, encoding="ISO-8859-1") as f:
            for line in f:
                fields = line.split('\t')
                movieNames[int(fields[0])] = fields[1]
    return movieNames
'''


spark = SparkSession.builder \
    .appName("buuble-breaker") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()

sc=spark.sparkContext

#dataRDD = sc.textFile('s3a://gdelt-open-data/events/201[4-8]*')
mentionRDD = sc.textFile(sys.argv[1])
mentionRDD = mentionRDD.map(lambda x: x.encode("utf", "ignore"))
mentionRDD.cache()
mentionRDD  = mentionRDD.map(lambda x : x.split('\t'))
mentionRowRDD = mentionRDD.map(lambda x : Row(event_id = x[0],
                                    mention_id = x[5],
                                    mention_doc_tone = x[13],
                                    mention_time_date = x[2],
                                    event_time_date = x[1],
                                    mention_src_name = x[4]))

gkgRDD = sc.textFile(sys.argv[2])
gkgRDD = gkgRDD.map(lambda x: x.encode("utf", "ignore"))
gkgRDD.cache()
gkgRDD = gkgRDD.map(lambda x: x.split('\t'))
gkgRowRDD = gkgRDD.map(lambda x : Row(src_common_name = x[3],
                                    doc_id = x[4],
                                    themes = x[7]))



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

joinedDF = mentionDF.join(gkgDF, mentionDF("mention_id") === gkgDF("doc_id"), "inner")#.select("code", "date")
joinedDF.show()


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
