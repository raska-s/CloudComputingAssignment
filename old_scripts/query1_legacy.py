# -*- coding: utf-8 -*-

# In[1]:
import findspark
findspark.init()
findspark.find()


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import udf
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql.functions import count, avg
from pyspark.sql.window import Window

from lib import tools
from lib import geoUtils

conf = pyspark.SparkConf().setAppName('SparkApp').set("spark.executor.memory", "9g").set("spark.driver.memory", "9g")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
#%%
from pyspark.sql.functions import *
from pyspark.sql.types import *
schema = StructType([
    StructField("medallion", StringType(), True),
    StructField("hack_license", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("trip_time_in_secs", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("surcharge", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
                    ])
# PATH = "/root/sorted_data*.csv"
# PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\sorted_data*.csv"
PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\shuffled_data*.csv"
# PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\repository\\CloudComputingAssignment\\StreamOut"

data = spark.read.option("header", "false").schema(schema).csv(PATH)
# In[ ]:

# The cells for this query are squares of 500 m X 500 m. The cell grid starts with cell 1.1, 
# located at 41.474937, -74.913585 (in Barryville). The coordinate 41.474937, -74.913585 marks
# the center of the first cell. Cell numbers increase towards the east and south, with the shift
# to east being the first and the shift to south the second component of the cell, i.e., cell 3.7
# is 2 cells east and 6 cells south of cell 1.1. The overall grid expands 150km south and 150km
# east from cell 1.1 with the cell 300.300 being the last cell in the grid. All trips starting or
# ending outside this area are treated as outliers and must not be considered in the result computation.

# In[ ]:

convertToCell_udf = udf(lambda lat, lon: geoUtils.convertToCell(lat,lon), T.StringType())
data = data.withColumn('cell_start', convertToCell_udf('pickup_latitude', 'pickup_longitude'))\
    .withColumn('cell_end', convertToCell_udf('dropoff_latitude', 'dropoff_longitude'))
data= data.filter((data.cell_start !='999.999') & (data.cell_end !='999.999'))

assignRouteID_udf = udf(lambda lat_start, lon_start, lat_end, lon_end: geoUtils.assignRouteID(lat_start, lon_start, lat_end, lon_end), T.StringType())
data = data.withColumn('origin->dest', assignRouteID_udf('pickup_latitude', 'pickup_longitude','dropoff_latitude', 'dropoff_longitude'))\
    

# In[ ]:
windowedData = data \
    .groupBy(window(data['dropoff_datetime'], "30 minutes", "30 minutes"), data['origin->dest']) \
    .count()
# In[ ]:
    
from pyspark.sql.window import Window
w = Window.partitionBy('window').orderBy(col('count').desc())
indexer = Window.partitionBy(lit(1)).orderBy(lit(1))

rankedData = windowedData.orderBy(col('window').asc()).withColumn("rank", row_number().over(w)).filter(col('rank')<=10)
rankedData = rankedData.withColumn("windowID", col("window").getField("end"))
rankedData = rankedData.orderBy(col('windowID').asc(), col('rank').asc())

windowCountData = windowedData.withColumn('tripsPerWindow', F.sum('count').over(w))
windowCountData = windowCountData.dropDuplicates(['window'])
windowCountData = windowCountData.withColumn("windowID", col("window").getField("end"))

# UNCOMMENT BELOW TO PRINT EACH WINDOW TO CONSOLE - output might not be time-wise ordered as dataframe is distributed
# --------------------------------------------------------------------------------------
# windowList = rankedData.select(col('windowID').cast('string')).distinct().toPandas()
# windowList = windowList['windowID'].astype('str')

# for windowName in windowList:
    # rankedData.filter(rankedData.windowID.contains(windowName)).show()
# --------------------------------------------------------------------------------------

# In[ ]:
countOutput = windowCountData.toPandas()
rawOutput = rankedData.toPandas()
# In[ ]:

import pandas as pd
import datetime as dt
import matplotlib.dates as mdates

rawOutput['windowID'] = rawOutput['windowID'].astype(str)
rawOutput['endtime']= rawOutput['windowID'].str[11:16]
sorting = rawOutput.groupby(['endtime', 'origin->dest'], as_index=False)['count'].mean()
sorting['localRank'] = sorting.groupby('endtime')['count'].rank(ascending=False, method = 'first')

# n. average trips in a 30min window for the dataset
countOutput['windowID'] = countOutput['windowID'].astype(str)
countOutput['endtime']= countOutput['windowID'].str[11:16]
countSorting = countOutput.groupby(['endtime'], as_index=False)['count'].mean() 
countSorting['endtime'] = pd.to_datetime(countSorting['endtime'], format = '%H:%M')

# top routes for each day in a 30min window for the dataset
topRoutesInDay = sorting[sorting['localRank']<=10]
topRoutesInDay = topRoutesInDay.sort_values(by=['endtime','localRank'], ascending=[True, True]) # n. average trips for a route in a 30 min window

import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (14,3)

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator())
plt.plot(countSorting['endtime'],countSorting['count'])
plt.fill_between(countSorting['endtime'], countSorting['count'], color='#539ecd')
plt.xticks(countSorting['endtime'], rotation='vertical')
plt.title('NYC taxi trip busiest times')
plt.xlabel('Time of day')
plt.ylabel('Average routes taken ')

# plt.rcParams["figure.figsize"] = plt.rcParamsDefault["figure.figsize"] # Reset figsize
# DONE: conv to pandas, save to CSV
#DONE: show per partition

#DONE: Rank per window and limit
#DONE: Aggregate windowedData and display windows

# query=windowedData.writeStream\
#      .queryName("t1_stream")\
#      .foreachBatch(batchFunction)\
#      .outputMode('complete')\
#      .option("truncate", False)\
#      .start().awaitTermination()
     # .trigger(processingTime='5 seconds')\

# from IPython.display import display, clear_output
# from time import sleep

# while True:
#     clear_output(wait=True)
#     display(query.status)
#     display(spark.sql('SELECT * FROM t1_stream').show())
#     sleep(10)

# In[ ]:


# query2 = windowedData.select('window').collect() \
#     .writeStream.format("memory") \
#     .queryName("window_stream") \
#     .outputMode("complete") \
#     .option("truncate", False) \
#     .start()

# query2.awaitTermination()
# window_array = [row.window for row in window_list]


# In[ ]:


# from IPython.display import display, clear_output
# from time import sleep


# while True:
#     clear_output(wait=True)
# #     display(query.status)
#     display(spark.sql('SELECT * FROM t1_stream WHERE count=5 ').show(10))
#     sleep(1)


# In[ ]:



# frequentRoutes = windowedData.groupBy(["route_ID"]).agg(count("*")).orderBy("count(1)",ascending=False)

# .orderBy("count(1)",ascending=False)
# .withWatermark("trip_endtime", "30 minutes")


# In[ ]:


# windowedData.printSchema()


# In[ ]:





# In[ ]:
