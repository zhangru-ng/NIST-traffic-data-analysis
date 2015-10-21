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
# outputFilePath = "/local/data/traffic/lane/baselinenoinfo/cleaning_subm_14_04_baselinenoinfo_01.csv"
testFilePath = sys.argv[1]
outputFilePath = sys.argv[2]

print("Current date & time " + time.strftime("%c") + ": Computing NoInformation Baseline for " + testFilePath)




# Read in the test file
detectorData = pd.read_csv(testFilePath)

# Since no change, just output the traffic flow, which is the volume attribute
outputData = detectorData['flow']

outputFileDir = os.path.dirname(outputFilePath)
# make output directory if needed
if not os.path.exists(outputFileDir):
    os.mkdir(outputFileDir)

# Write to the key file
# For now, include the headers
outputData.to_csv(outputFilePath,index=False)