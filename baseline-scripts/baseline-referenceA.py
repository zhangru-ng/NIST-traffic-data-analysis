#!/Users/pcf/anaconda/bin/python

import sys
import numpy as np
import pandas as pd
import math
import re
import string
import os
import time

# Check that we have enough arguments
if len(sys.argv) < 3:
    print("Must provide two arguments: ./baseline-noinfo.py testFilePath outputFilePath")
    sys.exit(1)
    

# For testing, we will feed these in for now
# testFilePath = "/local/data/traffic/lane/test/cleaning_test_14_04.csv"
# outputFileDir = "/local/data/traffic/lane/baselinereferenceA/cleaning_subm_baselinereferenceA_01.csv"
testFilePath = sys.argv[1]
outputFilePath = sys.argv[2]

print("Current date & time " + time.strftime("%c") + ": Computing ReferenceA Baseline for " + testFilePath)





# Read in the test file
detectorData = pd.read_csv(testFilePath)

# There are a variety of options for processing here
# Option 1: First sort the data by lane_id and then by measurement stamp
# With this, we can stream through the entire data frame, checking for 
# the same lane to reset
# Option 2: We can do it by lane id and create a view (not a copy) so that
# the original data frame is changed when we make a modification
# We go with option 2 since we slice by lane, and each lane is
# sorted by measurement_start


laneIdList = detectorData['lane_id'].value_counts().index.tolist()
# Approximation 1: Using measurement_start string without date filtering
# Given the format of the timestamps, this should be OK

# Set threshold based on the standard deviation of the data as a whole
volumeSD = detectorData['flow'].std(axis=0)
# This constant is the important threshold for a DET-like analysis
sdThresh = 1
# Another constant that can influence the decision: the side of the window
# to take the median around in each direction
medianWindowSize = 4
# It might be better to do vectorized programming rather
# than the iterations for speed, but this should suffice for now.
for currLane in laneIdList:
    # use a pandas slice to get the current volume as a list without
    # changing the order, which is sorted by measurement_start 
    # as per specified in the test data
    volumeVec = detectorData.loc[detectorData['lane_id'] == currLane,'flow'].tolist()
    # Use the flow to produce updated corrections based
    # on a two sided median
    vecLen = len(volumeVec)
    for pos in xrange(0, vecLen):
        # The iterator produces them in order
        # Approximation 2: Apply the process ignoring missing entries. While 
        # it may be desirable for cleaning to be skipped if there are lots of
        # missing timestamps in between, we ignore this critical feature
        if((pos >= medianWindowSize) & (pos <= (vecLen - (1+medianWindowSize)))):
            # colume 3 is volume
            currMedian = np.median(volumeVec[pos-medianWindowSize:pos+medianWindowSize])
            if( (volumeVec[pos] <= currMedian - sdThresh*volumeSD  ) 
                | (volumeVec[pos] >= currMedian + sdThresh*volumeSD)):
                # If here, replace with the median
                volumeVec[pos] = int(round(currMedian,0))
    # Assign the current slice to detectorData
    detectorData.loc[detectorData['lane_id'] == currLane,'flow'] = volumeVec           

# Since we didn't sort the original data, no sorting should be required
# We just need to output the volume data
outputData = detectorData['flow']

# make output directory if needed
outputFileDir = os.path.dirname(outputFilePath)
if not os.path.exists(outputFileDir):
    os.mkdir(outputFileDir)

# Write to the key file
# For now, include the headers
outputData.to_csv(outputFilePath,index=False)