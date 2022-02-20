# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 22:06:54 2022
Unused script that uses DStream to calculate and process operations
@author: raska
"""
import findspark
findspark.init()
# import pyspark
findspark.find()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.streaming import StreamingContext
from lib import geoUtils


from pyspark.sql.functions import udf
from pyspark.sql import types as T

take=["dropoff_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"]

conf = pyspark.SparkConf().setAppName('SparkApp').set("spark.executor.memory", "6g").set("spark.driver.memory", "6g")

sc = pyspark.SparkContext(conf=conf)
ssc = StreamingContext(sc, 3)
dataloc = 'C:\\Users\\raska\\Cranfield data\\cloud computing\\repository\\CloudComputingAssignment\\splitStreamOut'

# Dropoff Time Processing
dropoffTimeStreamLoc = str(dataloc)+'\\'+str(take[0])
print('streaming from ', dropoffTimeStreamLoc)
dropoffTime = ssc.textFileStream(dropoffTimeStreamLoc)
# dropoffTime.pprint()
streamSize = dropoffTime.count()


#Dropoff latitude processing
dropoffLatStreamLoc = str(dataloc)+'\\'+str(take[4])
print('streaming from ', dropoffLatStreamLoc)
dropoffLat = ssc.textFileStream(dropoffLatStreamLoc)
dropoffLatFl = dropoffLat.map(lambda x:[x]).map(lambda x: float(x[0]))
dropoffY=dropoffLatFl.map(lambda x:geoUtils.convertLat(x))\
    .transform(lambda rdd: rdd.zipWithIndex().map(lambda x: (x[1], x[0])))

#Dropoff longitude processing
dropoffLongStreamLoc = str(dataloc)+'\\'+str(take[3])
print('streaming from ', dropoffLongStreamLoc)
dropoffLong = ssc.textFileStream(dropoffLongStreamLoc)
dropoffLongFl = dropoffLong.map(lambda x:[x]).map(lambda x: float(x[0]))
dropoffX=dropoffLongFl.map(lambda x:geoUtils.convertLon(x))\
    .transform(lambda rdd: rdd.zipWithIndex().map(lambda x: (x[1], x[0])))\
    

    
#Pickup latitude processing
pickupLatStreamLoc = str(dataloc)+'\\'+str(take[2])
print('streaming from ', pickupLatStreamLoc)
pickupLat = ssc.textFileStream(pickupLatStreamLoc)
pickupLatFl = pickupLat.map(lambda x:[x]).map(lambda x: float(x[0]))
pickupY=pickupLatFl.map(lambda x:geoUtils.convertLat(x))\
    .transform(lambda rdd: rdd.zipWithIndex().map(lambda x: (x[1], x[0])))

#Pickup longitude processing
pickupLongStreamLoc = str(dataloc)+'\\'+str(take[1])
print('streaming from ', pickupLongStreamLoc)
pickupLong = ssc.textFileStream(pickupLongStreamLoc)
pickupLongFl = pickupLong.map(lambda x:[x]).map(lambda x: float(x[0]))
routeID=pickupLongFl.map(lambda x:geoUtils.convertLon(x))\
    .transform(lambda rdd: rdd.zipWithIndex().map(lambda x: (x[1], x[0])))\
    .join(pickupY).join(dropoffX).join(dropoffY)



ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate