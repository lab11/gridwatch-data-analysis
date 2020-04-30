#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, asc, desc, lead, lag, udf, hour, month, dayofmonth, dayofyear, collect_list, lit, year, date_trunc, dayofweek, when, unix_timestamp, array
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType, DateType, TimestampType, LongType
from pyspark import SparkConf
from datetime import datetime, timedelta
import os
from math import isnan
import argparse
import json
import calendar

#read arguments
parser = argparse.ArgumentParser()
parser.add_argument('result')
parser.add_argument('user')
parser.add_argument('password')
parser.add_argument('bucket')
args = parser.parse_args()

#initiate spark context
spark = SparkSession.builder.appName("SAIDI/SAIFI cluster size").getOrCreate()

### It's really important that you partition on this data load!!! otherwise your executors will timeout and the whole thing will fail
start_time = '2018-07-01'
end_time = '2019-09-01'
cluster_distance_seconds = 180
CD = cluster_distance_seconds

#Roughly one partition per week of data is pretty fast and doesn't take too much chuffling
num_partitions = int((datetime.strptime(end_time,"%Y-%m-%d").timestamp() - datetime.strptime(start_time,"%Y-%m-%d").timestamp())/(7*24*3600))

# This builds a list of predicates to query the data in parrallel. Makes everything much faster
start_time_timestamp = calendar.timegm(datetime.strptime(start_time, "%Y-%m-%d").timetuple())
end_time_timestamp = calendar.timegm(datetime.strptime(end_time, "%Y-%m-%d").timetuple())
stride = (end_time_timestamp - start_time_timestamp)/num_partitions
predicates = []
for i in range(0,num_partitions):
    begin_timestamp = start_time_timestamp + i*stride
    end_timestamp = start_time_timestamp + (i+1)*stride
    pred_string = "time >= '" + datetime.utcfromtimestamp(int(begin_timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    pred_string += "' AND "
    pred_string += "time < '" + datetime.utcfromtimestamp(int(end_timestamp)).strftime("%Y-%m-%d %H:%M:%S") + "'"
    predicates.append(pred_string)

#This query should only get data from deployed devices in the deployment table
query = ("""
    (SELECT powerwatch.core_id, time, is_powered, product_id, millis, last_unplug_millis,
            last_plug_millis, d.location_latitude, d.location_longitude, d.site_id FROM
    powerwatch
    INNER JOIN (
      SELECT core_id,
        location_latitude,
        location_longitude,
        CAST(site_id as INTEGER) as site_id,
        COALESCE(deployment_start_time, '1970-01-01 00:00:00+0') as st,
        COALESCE(deployment_end_time, '9999-01-01 00:00:00+0') as et
      FROM deployment) d ON powerwatch.core_id = d.core_id
    WHERE time >= st AND time <= et AND site_id < 100 AND """ +
        "time >= '" + start_time + "' AND " +
        "time < '" + end_time + "' AND " +
        "(product_id = 7008 OR product_id = 7009 or product_id = 7010 or product_id = 7011 or product_id = 8462)) alias")

pw_df = spark.read.jdbc(
            url = "jdbc:postgresql://timescale.ghana.powerwatch.io/powerwatch",
            table = query,
            predicates = predicates,
            properties={"user": args.user, "password": args.password, "driver":"org.postgresql.Driver"})

#if you have multiple saves below this prevents reloading the data every time
pw_df.cache()

#We should mark every row with the number of unique sensors reporting in +-5 days so we now the denominator for SAIDI/SAIFI
pw_distinct_core_id = pw_df.select("time","core_id")
pw_distinct_core_id = pw_distinct_core_id.groupBy(F.window("time", '10 days', '1 day')).agg(F.countDistinct("core_id"),F.array_distinct(F.collect_list("core_id")).alias("core_ids_reporting"))
pw_distinct_core_id = pw_distinct_core_id.withColumn("time", F.from_unixtime((F.unix_timestamp(col("window.start")) + F.unix_timestamp(col("window.end")))/2))
pw_distinct_core_id = pw_distinct_core_id.select(col("count(DISTINCT core_id)").alias("sensors_reporting"), "time","core_ids_reporting")
pw_distinct_core_id = pw_distinct_core_id.withColumn("day",F.date_trunc("day","time"))
pw_distinct_core_id = pw_distinct_core_id.select("day","sensors_reporting","core_ids_reporting")

pw_powered_locations = pw_df.select("time","is_powered","core_id","location_latitude","location_longitude")
pw_powered_locations = pw_powered_locations.withColumn("is_powered",col("is_powered").cast(IntegerType()))
pw_powered_locations = pw_powered_locations.groupBy("core_id",F.window("time",'4 minutes', '1 minute')).agg(F.avg("is_powered").alias("avg_power"),
                                                                                                            F.first("location_latitude").alias("location_latitude"),
                                                                                                            F.first("location_longitude").alias("location_longitude"))

pw_powered_locations = pw_powered_locations.filter(col("avg_power") == 1)
pw_powered_locations = pw_powered_locations.withColumn("time", col("window.start"))
pw_powered_locations = pw_powered_locations.select("time","core_id","location_latitude","location_longitude")
pw_powered_locations = pw_powered_locations.withColumn("loc_struct",F.struct("core_id","location_latitude","location_longitude"))
pw_powered_locations = pw_powered_locations.groupBy("time").agg(F.collect_list("loc_struct").alias("loc_struct"))
pw_powered_locations = pw_powered_locations.select(col("time").alias("minute"),"loc_struct")

#now we need to created a window function that looks at the leading lagging edge of is powered and detects transitions
#then we can filter out all data that is not a transition
w = Window.partitionBy("core_id").orderBy(asc("time"))
pw_df = pw_df.withColumn("previous_power_state", lag("is_powered").over(w))

#filter out every time that the state does not change
pw_df = pw_df.filter(col("previous_power_state") != col("is_powered"))

#now we should only count this if it is an outage (on, off, on)
is_powered_lead = lead("is_powered",1).over(w)
is_powered_lag = lag("is_powered",1).over(w)
pw_df = pw_df.withColumn("lagging_power",is_powered_lag)
pw_df = pw_df.withColumn("leading_power",is_powered_lead)
pw_df = pw_df.withColumn("outage", when((col("is_powered") == 0) & (col("lagging_power") == 1) & (col("leading_power") == 1), 1).otherwise(0))

#now need the most accurate outage time possible for outage event
#now find all the exact outage and restore times using millis
def timeCorrect(time, millis, unplugMillis):
    if(unplugMillis == 0 or millis == None or unplugMillis == None or isnan(millis) or isnan(unplugMillis)):
        return time
    elif unplugMillis > millis:
        return time
    else:
        return time - timedelta(microseconds = (int(millis)-int(unplugMillis))*1000)
udftimeCorrect = udf(timeCorrect, TimestampType())
pw_df = pw_df.withColumn("outage_time", udftimeCorrect("time","millis","last_unplug_millis"))
pw_df = pw_df.withColumn("outage_time", F.unix_timestamp("outage_time"))
pw_df = pw_df.withColumn("r_time", udftimeCorrect("time","millis","last_plug_millis"))
pw_df = pw_df.withColumn("r_time", F.unix_timestamp("r_time"))

#now denote the end time of the outage for saidi reasons
time_lead = lead("r_time",1).over(w)
pw_df = pw_df.withColumn("restore_time", time_lead)

#now filter out everything that is not an outage. We should have a time and end_time for every outage
pw_df = pw_df.filter("outage != 0")
pw_df = pw_df.withColumn("minute",F.date_trunc("minute", F.from_unixtime("outage_time")))
pw_df = pw_df.withColumn("day",F.date_trunc("day", F.from_unixtime("outage_time")))
pw_df = pw_df.join(pw_powered_locations,pw_df.minute == pw_powered_locations.minute, how='left')
pw_df = pw_df.join(pw_distinct_core_id,pw_df.day == pw_distinct_core_id.day, how='left')
pw_df = pw_df.select("core_id","time","outage_time","restore_time","location_latitude","location_longitude",F.explode(col("loc_struct")).alias("powered_sensors"),"sensors_reporting","core_ids_reporting")
pw_df.repartition(1).write.parquet(args.result + '/outage_transitions',mode='overwrite',compression='gzip')
