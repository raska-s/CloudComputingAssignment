# -*- coding: utf-8 -*-
import time
import pandas as pd
import os
import numpy as np
from os import listdir
from os.path import isfile, join

class streamSimulator():
    def streamCSV(mainFile, folderName, dropColumns=None, batchSize = 100, timeInterval=1, fileWindow=None):
        idx = 0
        print('Streaming data ... \n Batchsize:', batchSize, '\n From CSVfile at: ', mainFile, '\n to directory .\\', folderName, '\n')
        for chunk in pd.read_csv(mainFile, chunksize=batchSize, index_col=False, header=None):
            if dropColumns is not None:
                chunk = chunk.drop(dropColumns, axis=1)
            os.makedirs(folderName, exist_ok=True)  
            # chunk.to_csv('folder/subfolder/out.csv')  
            fname = str(folderName)+'/stream_'+str(idx)+'.csv'
            chunk.to_csv(fname, index=False, header=None)

            if fileWindow is not None:
                if idx >= 0:
                    streamSimulator.deleteJunk(idx-fileWindow, folderName)
            idx+=1
            time.sleep(timeInterval)
        print('Sent ', idx, ' streams.')
    
    def deleteJunk(idx, foldername):
        fname = str(foldername)+'/stream_'+str(idx)+'.csv'
        if(os.path.exists(fname) and os.path.isfile(fname)):
          os.remove(fname)
          
    def splitStreamCSV(mainFile, headers, takeHeaders=None, batchSize = 100, timeInterval=1, fileWindow=None):
        if takeHeaders is None:
            print('streamError: No takeHeaders specified!')
        else:
            idx = 0
            saveToDir = 'splitStreamOut'
            os.makedirs(saveToDir, exist_ok=True)
            fullData = pd.read_csv(mainFile, chunksize=batchSize, index_col=False)
            # fullData = fullData.sample(frac=1).reset_index(drop=True)
            print('Streaming data ... \n Batchsize:', batchSize, '\n From CSVfile at: ', mainFile, '\n To directory at: HERE\\', str(takeHeaders), '\n')
            for chunk in fullData:
                chunk.columns = headers
                for eachColumn in takeHeaders:
                    localDir = str(saveToDir)+'/'+str(eachColumn)
                    valChunk = chunk[eachColumn]
                    os.makedirs(localDir, exist_ok=True)
                    fname = str(localDir)+'/stream_'+str(idx)+'.csv'
                    valChunk.to_csv(fname, index=False, header=None)
                    if fileWindow is not None:
                        if idx >= 0:
                            streamSimulator.deleteJunk(idx-fileWindow, localDir)
                idx+=1
                time.sleep(timeInterval)
            print('Sent ', idx, ' streams.')
            pass
            
    def sortCSV(farePath, tripPath, readRows=1000):
        fareFileNameList = [f for f in listdir(farePath) if isfile(join(farePath, f))]
        tripFileNameList = [f for f in listdir(tripPath) if isfile(join(tripPath, f))]
        # data = pd.read_csv(fileLoc, index_col=False, header=None, nrows = 1000)
        selHeaders=["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]
        appendedData = []
        for fareFile, tripFile in zip(fareFileNameList, tripFileNameList):
            fareFileLoc = str(farePath)+'\\'+str(fareFile)  
            tripFileLoc = str(tripPath)+'\\'+str(tripFile)  
            fareData = pd.read_csv(fareFileLoc, header=0, nrows= readRows)
            tripData= pd.read_csv(tripFileLoc, header=0, nrows = readRows)
            # print(tripData.columns)
            colsToUse = fareData.columns.difference(tripData.columns)
            combined = pd.merge(tripData, fareData[colsToUse], left_index=True, right_index=True, how='outer')
            # print(combined.columns)
            combined.columns = combined.columns.str.replace(' ', '')
            clean = combined[selHeaders]
            clean = clean.loc[:,~clean.columns.duplicated()]
            appendedData.append(clean)
        appendedData = pd.concat(appendedData)
        return appendedData
        # print(onlyfiles)
        # combined_csv = pd.concat( [ pd.read_csv(f) for f in filenames ] )
        #     data = pd.read_csv(fileLoc, index_col=False, header=None, nrows = 1000)
        #     data = data.sample(frac=20, replace=True).reset_index(drop=True)
        #     appendedData.append(data)
        # appendedData = pd.concat(appendedData)
class geoUtils():
    def assignRouteID(lat_start, lon_start, lat_end, lon_end):
        """
        Legacy function (not in use)

        Parameters
        ----------
        lat_start : TYPE
            DESCRIPTION.
        lon_start : TYPE
            DESCRIPTION.
        lat_end : TYPE
            DESCRIPTION.
        lon_end : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        cellStartLatitude = 41.474937
        cellStartLongitude = -74.91358
        cellLatitudeSize = 0.004491556
        cellLongitudeSize = 0.005986
        latUnit_start = int(np.floor(np.abs(cellStartLatitude-lat_start)/cellLatitudeSize))
        lonUnit_start = int(np.floor((lon_start-cellStartLongitude)/cellLongitudeSize))
        latUnit_end = int(np.floor(np.abs(cellStartLatitude-lat_end)/cellLatitudeSize))
        lonUnit_end = int(np.floor((lon_end-cellStartLongitude)/cellLongitudeSize))
        if latUnit_start<=300 and lonUnit_start<=300 and latUnit_end<=300 and lonUnit_end<=300:
            return str(lonUnit_start)+'.'+str(latUnit_start)+'->'+str(lonUnit_end)+'.'+str(latUnit_end)
        else:
            return str(999.999)+'->'+str(999.999)
        
    def convertToCell(lat, lon):
        """


        Parameters
        ----------
        lat : TYPE
            DESCRIPTION.
        lon : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        cellStartLatitude = 41.474937
        cellStartLongitude = -74.91358
        cellLatitudeSize = 0.004491556
        cellLongitudeSize = 0.005986
        latUnit = int(np.floor(np.abs(cellStartLatitude-lat)/cellLatitudeSize))
        lonUnit = int(np.floor((lon-cellStartLongitude)/cellLongitudeSize))
        if lonUnit<=300 and latUnit<=300:
            return str(lonUnit)+'.'+str(latUnit)
        else:
            return str(999.999)
        
    def convertLat(lat):
        """
        Converts latitude into cell

        Parameters
        ----------
        lat : float
            Latitude.

        Returns
        -------
        float
            Y - cell of latitude.

        """
        cellStartLatitude = 41.474937
        cellLatitudeSize = 0.004491556
        latUnit = int(np.floor(np.abs(cellStartLatitude-lat)/cellLatitudeSize))
        if latUnit<=300:
            return latUnit
        else:
            return 999
        
    def convertLon(lon):
        """
        Converts longitude into cell

        Parameters
        ----------
        lon : float
            Longitude.

        Returns
        -------
        float
            X - cell of longitude.

        """
        cellStartLongitude = -74.91358
        cellLongitudeSize = 0.005986
        lonUnit = int(np.floor((lon-cellStartLongitude)/cellLongitudeSize))
        if lonUnit<=300:
            return lonUnit
        else:
            return 999

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import udf
from pyspark.sql import types as T
import pyspark.sql.functions as F

from pyspark.sql.window import Window
from pyspark.sql import SQLContext 


conf = pyspark.SparkConf().setAppName('SparkApp').set("spark.executor.memory", "9g").set("spark.driver.memory", "9g")
sc = pyspark.SparkContext(conf=conf)
sql = SQLContext(sc)
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
PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\sorted_data*.csv"
# PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\shuffled_data*.csv"
# PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\repository\\CloudComputingAssignment\\StreamOut"

data = spark.read.format("memory").option("header", "false").option("maxFilesPerTrigger", "1").schema(schema).csv(PATH)
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
data = data.withColumn('cell_start', convertToCell_udf('pickup_latitude', 'pickup_longitude'))
data = data.withColumn('cell_end', convertToCell_udf('dropoff_latitude', 'dropoff_longitude'))

assignRouteID_udf = udf(lambda lat_start, lon_start, lat_end, lon_end: geoUtils.assignRouteID(lat_start, lon_start, lat_end, lon_end), T.StringType())
data = data.withColumn('origin->dest', assignRouteID_udf('pickup_latitude', 'pickup_longitude','dropoff_latitude', 'dropoff_longitude'))

# In[ ]:
data = data.filter((data.cell_start !='999.999') & (data.cell_end !='999.999'))
# data.printSchema()

# In[ ]:
# The goal of this query is to identify areas that are currently most profitable for taxi drivers. 
#
# The profitability of an area is determined by dividing the area profit by the number of empty taxis 
# in that area within the last 15 minutes. 
# 
# The profit that originates from an area is computed by calculating the median fare + tip for trips that 
# started in the area and ended within the last 15 minutes. 
# 
# The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less 
# than 30 minutes ago and had no following pickup yet within 30 minutes.

# In[ ]:
    
# windowCountData = windowCountData.withColumn("windowID", col("window").getField("end"))

lastDropoffData = data \
    .groupBy(window(data['dropoff_datetime'], "15 minutes", "15 minutes"), data['cell_end']) \
    .count() \
    .withColumn("windowID", col("window").getField("end"))\
    .selectExpr("windowID as window",  "cell_end as cell", "count as dropoff_count")

lastPickupData = data \
    .groupBy(window(data['pickup_datetime'], "15 minutes", "15 minutes"), data['cell_start']) \
    .count()\
    .withColumn("windowID", col("window").getField("start"))\
    .selectExpr("windowID as window",  "cell_start as cell", "count as pickup_count")

# number of active empty taxis for each area in a window
emptyCabsInArea = lastDropoffData.join(lastPickupData, ['window', 'cell'], how = 'leftouter') \
    .na.fill(value=0)\
    .withColumn('empty_cabs', col('dropoff_count')-col('pickup_count'))\
    .filter(col('empty_cabs') > '0').drop('dropoff_count', 'pickup_count')

# emptyCabsInArea.show()

# DONE: Figure out the emptycabs problem (left anti does not output the right ones)
# DONE: n cabs dropping off at cell - n cabs picking up (also n empty cabs cannot be zero since exception)
# In[ ]:
from pyspark.sql import DataFrameStatFunctions as statFunc
minutesInterval = 15
secondsInterval = minutesInterval*60
from pyspark.sql import SQLContext 

tripsInLastWindow = data.where(data.trip_time_in_secs <= str(secondsInterval)) \
    .withColumn("total_profit", col("fare_amount")+col("tip_amount"))
magic_percentile = F.expr('percentile_approx(total_profit, 0.5)')
areaProfit = tripsInLastWindow.groupBy(window(data['dropoff_datetime'], "15 minutes", "15 minutes"), data['cell_start']) \
    .agg(magic_percentile.alias('profit'))\
    .withColumn("windowID", col("window").getField("end"))\
    .selectExpr("windowID as window",  "cell_start as cell", "profit as profit")
    
win = Window.partitionBy('window').orderBy(col('profitability').desc())    
win2 = Window.partitionBy('window')

profitabilityData = emptyCabsInArea.join(areaProfit, ['window', 'cell'])\
    .withColumn("profitability", col('profit')/col('empty_cabs'))\
    .drop('empty_cabs', 'profit')\

profitabilityFinal = profitabilityData.orderBy(col('window').asc())\
    .withColumn("rank", row_number().over(win))\
    .filter(col('rank')<=10)
    
profitabilityPerWindow = profitabilityData.withColumn('avg_profitability', F.mean('profitability').over(win2))\
    .orderBy(col('window').asc()).drop('cell', 'profitability')\
    .dropDuplicates(['window'])\
    
# rankedData = windowedData.orderBy(col('window').asc()).withColumn("rank", row_number().over(w)).filter(col('rank')<=10)
# rankedData = rankedData.withColumn("windowID", col("window").getField("end"))
# rankedData = rankedData.orderBy(col('windowID').asc(), col('rank').asc())

# profitabilityPerWindow.show()
# areaProfit.show()
# emptyCabsInArea.show()

# lastDropoffData.show()
# lastPickupData.show()
# pickupDropoffData.show()

# In[ ]:
profitabilityFinalOut = profitabilityFinal.select('window', 'cell',
                        'profitability', 'rank')
profitabilityFinalOut.coalesce(1).write.csv('profitabilityFinalOut')
profitabilityPerWindow.coalesce(1).write.csv('profitabilityPerWindowOut')

# In[ ]:
# TODO: Get profitability scale for time

# import pandas as pd
# import datetime as dt
# import matplotlib.dates as mdates

# finalOut['window'] = finalOut['window'].astype(str)
# finalOut['time']= finalOut['window'].str[11:16]
# sorting = finalOut.groupby(['time', 'cell'], as_index=False)['profitability'].mean()
# sorting['localRank'] = sorting.groupby('time')['profitability'].rank(ascending=False, method = 'first')

# # n. average trips in a 30min window for the dataset
# windowOut['window'] = windowOut['window'].astype(str)
# windowOut['time']= windowOut['window'].str[11:16]
# mostProfitableTimes = windowOut.groupby(['time'], as_index=False)['avg_profitability'].mean() 
# mostProfitableTimes['time'] = pd.to_datetime(mostProfitableTimes['time'], format = '%H:%M')

# # top most profitable cells for each day in a 30min window for the dataset
# topCellsInDay = sorting[sorting['localRank']<=10]
# topCellsInDay = topCellsInDay.sort_values(by=['time','localRank'], ascending=[True, True]) # n. average trips for a route in a 30 min window

# import matplotlib.pyplot as plt
# import matplotlib.ticker as plticker

# fig = plt.figure(figsize=(15,10))
# # ax.plot(x,y)
# fig, ax = plt.subplots()
# plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
# plt.gca().xaxis.set_major_locator(mdates.DayLocator())
# plt.subplots_adjust(top = 1, bottom = 0, right = 1, left = 0, 
#             hspace = 0, wspace = 0)
# plt.margins(0,0)
# plt.gca().xaxis.set_major_locator(plt.NullLocator())
# plt.gca().yaxis.set_major_locator(plt.NullLocator())
# ax.plot(mostProfitableTimes['time'],mostProfitableTimes['avg_profitability'])
# plt.fill_between(mostProfitableTimes['time'], mostProfitableTimes['avg_profitability'], color='#539ecd')
# plt.xticks(mostProfitableTimes['time'], rotation='vertical')
# plt.title('NYC taxi trip most profitable times')
# plt.xlabel('Time of day')
# plt.ylabel('Average profitability')
# plt.savefig('profitability_plot.pdf',bbox_inches='tight', dpi=150)  

# plt.rcParams["figure.figsize"] = plt.rcParamsDefault["figure.figsize"] # Reset figsize

    
