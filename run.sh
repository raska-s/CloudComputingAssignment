#!/bin/bash

## clear junk
echo 'Refreshing directory...'
rm -r rankedOut
rm -r windowCountOut

## Uncomment below to generate new data - may take some time!


#Running query 1
echo 'Submitting query 1..'
spark-submit --deploy-mode client query1.py --driver-memory 8g --executor-memory 8g
echo 'Query 1 complete. clearing junk..'

cat rankedOut/*.csv > rankings.csv
cat windowCountOut/*.csv >windowCount.csv

## mv rankedOut/*.csv rankings.csv
## mv windowCountOut/*.csv windowCount.csv

rm -r rankedOut
rm -r windowCountOut

echo 'Cleared junk. Submitting Query 2 ...' 

spark-submit --deploy-mode client query2.py --driver-memory 8g --executor-memory 8g

echo 'Clearing junk...'
mv profitabilityFinalOut/*.csv profitabilityOut.csv
mv profitabilityPerWindowOut/*.csv profitabilityPerWindow.csv

rm -r profitabilityPerWindowOut
rm -r profitabilityFinalOut

mkdir outputs
mv *.csv outputs
