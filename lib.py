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
        """
        Simulates the sreaming of a single CSV file

        Parameters
        ----------
        mainFile : string
            Filepath of CSV file.
        folderName : string
            File directory where data would be streamed to.
        dropColumns : array of strings, optional
            List of columns to drop in the streaming process. The default is None.
        batchSize : int, optional
            Number of rows intended to be sent in one stream. The default is 100.
        timeInterval : float, optional
            Number of seconds between streams. The default is 1.
        fileWindow : int, optional
            The maximum allowable number of files that stay in the directory in each stream process. None is default and corresponds to all files saved.

        Returns
        -------
        None.

        """
        
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
        """
        Deletes junk files

        Parameters
        ----------
        idx : int
            Order of file in stream.
        foldername : string
            Location of stream directory.

        Returns
        -------
        None.

        """
        fname = str(foldername)+'/stream_'+str(idx)+'.csv'
        if(os.path.exists(fname) and os.path.isfile(fname)):
          os.remove(fname)
          
    def splitStreamCSV(mainFile, headers, takeHeaders=None, batchSize = 100, timeInterval=1, fileWindow=None):
        """
        Streams a CSV file by splitting them into individual streams corresponding to the columns to be sent.

        Parameters
        ----------
        mainFile : string
            Source CSV file.
        headers : string
            Original headers of the CSV file.
        takeHeaders : array of strings, optional
            The selected columns to be sent - 5 columns mean there are 5 stream directories. The default is None.
        batchSize : int, optional
            number of rows to be sent in each stream. The default is 100.
        timeInterval : float, optional
            number of seconds between each stream. The default is 1.
        fileWindow : int, optional
            The maximum allowable number of files that stay in the directory in each stream process. None is default and corresponds to all files saved.


        Returns
        -------
        None.

        """
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
        """
        sorts a number of CSV files and makes a single file out of them

        Parameters
        ----------
        farePath : string
            fare data file path.
        tripPath : string
            trip data file path.
        readRows : int, optional
            number of rows to be read form each csv. The default is 1000.

        Returns
        -------
        appendedData : pandas dataframe
            result of sorting and merging.

        """
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
        Assigns route ID

        Parameters
        ----------
        lat_start : int
            Latitude of starting position.
        lon_start : int
            longitude of starting postion.
        lat_end : int
            latitude of ending position.
        lon_end : int
            longitude of ending position.

        Returns
        -------
        string
            string of start route -> end route.

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
        converts latitude and longitude into cell

        Parameters
        ----------
        lat : float
            latitude.
        lon : float
            latitude.

        Returns
        -------
        string
            cell ID.

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
