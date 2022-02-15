#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import socket
# from time import sleep

# host = '0.0.0.0'
# port = 12345

# s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.bind((host, port))
# s.listen(1)
# while True:
#     print('\nListening for a client at',host , port)
#     conn, addr = s.accept()
#     print('\nConnected by', addr)
#     try:
#         print('\nReading file...\n')
#         with open('C:\\Users\\raska\\Cranfield data\\cloud computing\\sorted_data.csv') as f:
#             for line in f:
#                 out = line.encode('utf-8')
#                 print('Sending line',line)
#                 conn.send(out)
#                 sleep(1)
#             print('End Of Stream.')
#     except socket.error:
#         print ('Error Occured.\n\nClient disconnected.\n')
# conn.close()


# In[1]:


import findspark
findspark.init()
# import pyspark
findspark.find()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
conf = pyspark.SparkConf().setAppName('SparkApp').set("spark.executor.memory", "6g").set("spark.driver.memory", "6g")

sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

# Define schema of the csv
# userSchema = StructType().add(“name”, “string).add(“salary”, “integer”)
# Read CSV files from set path
# dfCSV = spark.readStream.option(“sep”, “;”).option(“header, “false”).schema(userSchema).csv(“/tmp/text”)


# In[2]:


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
PATH = "sorted_data*.csv"
# PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\trip_data"



data = spark.readStream.format("memory").option("header", "false").schema(schema).csv(PATH)


# In[ ]:


data.isStreaming


# In[ ]:


data.printSchema()


# The cells for this query are squares of 500 m X 500 m. The cell grid starts with cell 1.1, located at 41.474937, -74.913585 (in Barryville). The coordinate 41.474937, -74.913585 marks the center of the first cell. Cell numbers increase towards the east and south, with the shift to east being the first and the shift to south the second component of the cell, i.e., cell 3.7 is 2 cells east and 6 cells south of cell 1.1. The overall grid expands 150km south and 150km east from cell 1.1 with the cell 300.300 being the last cell in the grid. All trips starting or ending outside this area are treated as outliers and must not be considered in the result computation.

# In[ ]:


import numpy as np
def assignRouteID(lat_start, lon_start, lat_end, lon_end):
    cellStartLatitude = 41.474937
    cellStartLongitude = -74.91358
    cellLatitudeSize = 0.004491556
    cellLongitudeSize = 0.005986
    latUnit_start = int(np.floor(np.abs(lat_start-cellStartLatitude)/cellLatitudeSize))
    lonUnit_start = int(np.floor((lon_start-cellStartLongitude)/cellLongitudeSize))
    latUnit_end = int(np.floor(np.abs(lat_end-cellStartLatitude)/cellLatitudeSize))
    lonUnit_end = int(np.floor((lon_end-cellStartLongitude)/cellLongitudeSize))
    if latUnit_start<=300 and lonUnit_start<=300 and latUnit_end<=300 and lonUnit_end<=300:
        return str(lonUnit_start)+'.'+str(latUnit_start)+'->'+str(lonUnit_end)+'.'+str(latUnit_end)
    else:
        return str(999.999)+'->'+str(999.999)
    
def convertToCell(lat, lon):
    cellStartLatitude = 41.474937
    cellStartLongitude = -74.91358
    cellLatitudeSize = 0.004491556
    cellLongitudeSize = 0.005986
    latUnit = int(np.floor(np.abs(lat-cellStartLatitude)/cellLatitudeSize))
    lonUnit = int(np.floor((lon-cellStartLongitude)/cellLongitudeSize))
    if lonUnit<=300 and latUnit<=300:
        return str(lonUnit)+'.'+str(latUnit)
    else:
        return str(999.999)
    
    
# convertToCell(40.71697, -73.956528)
from pyspark.sql.functions import udf
from pyspark.sql import types as T

convertToCell_udf = udf(lambda lat, lon: convertToCell(lat,lon), T.StringType())
data = data.withColumn('cell_start', convertToCell_udf('pickup_latitude', 'pickup_longitude'))
data = data.withColumn('cell_end', convertToCell_udf('dropoff_latitude', 'dropoff_longitude'))

assignRouteID_udf = udf(lambda lat_start, lon_start, lat_end, lon_end: assignRouteID(lat_start, lon_start, lat_end, lon_end), T.StringType())
data = data.withColumn('route_ID', assignRouteID_udf('pickup_latitude', 'pickup_longitude','dropoff_latitude', 'dropoff_longitude'))
# convertToCell(41.474937, -74.91358)
# convertToCell(44.2521, -75.9999)


# In[ ]:


data.printSchema()


# In[ ]:


# data = data.withColumn("trip_endtime", to_timestamp("dropoff_datetime"))
import pyspark.sql.functions as F
from pyspark.sql.functions import count, avg
from pyspark.sql.window import Window
data = data.filter((data.cell_start !='999.999') & (data.cell_end !='999.999'))

windowedData = data \
    .groupBy(window(data['dropoff_datetime'], "30 minutes", "30 minutes"), data['route_ID']) \
    .count()\
    .orderBy(col("window").asc(), col("count").desc())

# dataWindow = Window.partitionBy("window").orderBy(col("window").asc(), col("count").desc())

# def flatten_df(nested_df):
#     flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
#     nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

#     flat_df = nested_df.select(flat_cols +
#                                [F.col(nc+'.'+c).alias(nc+'_'+c)
#                                 for nc in nested_cols
#                                 for c in nested_df.select(nc+'.*').columns])
#     return flat_df

# flatWindow = flatten_df(windowedData)

# windowedData2=windowedData.withColumn("row",row_number().over('window_end')).filter(col("row") <= 10)
windowedData.printSchema()
# df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)


# In[ ]:


# streamData = windowedData.select('*', rank().over('window')).filter(col('count') <= 10).show() 

# df.select('*', rank().over(window).alias('rank')) 
#   .filter(col('rank') <= 2) 
#   .show() 

#   .format("parquet") 
#   .option("path", "/Users/abc/hb_parquet/data") 

# tumblingWindows = data.groupBy("route_ID", window("trip_endtime", "30 minutes")).count()
query=windowedData.writeStream.format("console")\
     .queryName("t1_stream")\
     .outputMode("complete")\
     .option("truncate", False)\
     .start().awaitTermination()
#     .partitionBy("window") \


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




