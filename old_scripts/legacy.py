# -*- coding: utf-8 -*-
"""
Created on Sat Feb 19 14:47:09 2022

@author: raska
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 19:42:38 2022

@author: raska
"""
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
        
class tools():
    def sparkDFShape(self):
        return (self.count(), len(self.columns))


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

from pyspark.sql.window import Window

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
PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\sorted_data*.csv"
# PATH = "C:\\Users\\raska\\Cranfield data\\cloud computing\\shuffled_data*.csv"
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
#     rankedData.filter(rankedData.windowID.contains(windowName)).show()
# --------------------------------------------------------------------------------------

# In[ ]:

windowCountOut = windowCountData.select('origin->dest', 'count',
                        'tripsPerWindow', 'windowID')
windowCountOut.coalesce(1).write.csv('windowCountOut')

rankedOut = rankedData.select('origin->dest', 'count',
                        'rank', 'windowID')
rankedOut.write.csv('rankedOut')

# rankedData.write.csv('rankedData.csv')
# In[ ]:

# import matplotlib.dates as mdates

# rawOutput['windowID'] = rawOutput['windowID'].astype(str)
# rawOutput['endtime']= rawOutput['windowID'].str[11:16]
# sorting = rawOutput.groupby(['endtime', 'origin->dest'], as_index=False)['count'].mean()
# sorting['localRank'] = sorting.groupby('endtime')['count'].rank(ascending=False, method = 'first')

# # n. average trips in a 30min window for the dataset
# countOutput['windowID'] = countOutput['windowID'].astype(str)
# countOutput['endtime']= countOutput['windowID'].str[11:16]
# countSorting = countOutput.groupby(['endtime'], as_index=False)['count'].mean() 
# countSorting['endtime'] = pd.to_datetime(countSorting['endtime'], format = '%H:%M')

# # top routes for each day in a 30min window for the dataset
# topRoutesInDay = sorting[sorting['localRank']<=10]
# topRoutesInDay = topRoutesInDay.sort_values(by=['endtime','localRank'], ascending=[True, True]) # n. average trips for a route in a 30 min window

# import matplotlib.pyplot as plt
# # plt.rcParams["figure.figsize"] = (14,3)
# fig = plt.figure(figsize=(15,10))
# plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
# plt.gca().xaxis.set_major_locator(mdates.DayLocator())
# plt.plot(countSorting['endtime'],countSorting['count'])
# plt.fill_between(countSorting['endtime'], countSorting['count'], color='#539ecd')
# plt.xticks(countSorting['endtime'], rotation='vertical')
# plt.title('NYC taxi trip busiest times')
# plt.xlabel('Time of day')
# plt.ylabel('Average no. of distinct routes taken')
# plt.savefig('time_plot.pdf',bbox_inches='tight', dpi=150)  
