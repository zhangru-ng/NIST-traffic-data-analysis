#!/usr/bin/env python
# -*- coding: utf-8 -*-

# scprit for cleaning inconsistent flow, speed , occupancy

import matplotlib.pyplot as plt
import pandas as pd

clean_0611 = pd.read_csv('/home/datascience/Desktop/cleaning-0611.csv',
names=['lane_id','measurement_start','speed','flow','occupancy','quality'])

# filter positive flow and speed
positive_flow_speed = (clean_0611['flow'] > 0) & (clean_0611['speed'] > 0)

# According to traffic flow theory
# 		v = c * q / o
# v -- space mean speed(in mph)
# q -- flow
# o -- occupancy
# c -- mean effective vehicle length(in feet) * 1.894
# note: effective vehicle length is the sum of actual vehicle length and detectable
# length of loop detector in feet, which is a constant
# For example:
# compact sedans length is about  14.75 feet
# truck length is about is about 50 feet
# The normal detectable length of loop detector is about 5 feet
# for sedans c is about 37, and for truck, c is about 104.17

# add new series 'coefficient' to store c, the constant mentioned above
clean_0611['coefficient'] = 0

clean_0611.loc[positive_flow_speed,['coefficient']] = clean_0611['speed'] * clean_0611['occupancy'] / clean_0611['flow']

# if the coefiicient is equal to 0, we can not get useful information
positive_coefficient = clean_0611[clean_0611['coefficient'] > 0]
# positive_coefficient.describe()
#        coefficient  
# count  1140416.000000  
# mean   31.590026  
# std    172.957404  
# min    0.015686  
# 25%    21.000000  
# 50%    24.400000  
# 75%    26.142857  
# max    12672.000000 

abnormal_coefficient = positive_coefficient[positive_coefficient['coefficient'] > 1000]
# abnormal_coefficient.describe()
#         coefficient  
# count   2710.000000  
# mean    2597.746409  
# std     2423.112885  
# min      500.294118  
# 25%      800.000000  
# 50%     1548.000000  
# 75%     3584.000000  
# max     12672.00000
