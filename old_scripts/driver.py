# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 20:16:52 2022
File used to handle CSV sorting and streaming. Now only used to sort and merge CSVs.
@author: raska
"""
from lib import streamSimulator

dataLoc = 'C:\\Users\\raska\\Cranfield data\\cloud computing\\shuffled_data.csv'
headernames = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]
dropped_cols =["medallion", "hack_license", "trip_time_in_secs", "trip_distance", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]
writeTo = 'StreamOut'
take=["dropoff_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"]
farePath='C:\\Users\\raska\\Cranfield data\\cloud computing\\trip_fare'
tripPath = 'C:\\Users\\raska\\Cranfield data\\cloud computing\\trip_data'


# streamSimulator.splitStreamCSV(data, headernames, takeHeaders=take, batchSize = 3, timeInterval=3, fileWindow=100)
testDat = streamSimulator.sortCSV(farePath, tripPath, readRows=10000)
testDat.to_csv(dataLoc, index=False, header=False)
# streamSimulator.streamCSV(dataLoc, writeTo, batchSize=100, timeInterval=2, fileWindow=None)