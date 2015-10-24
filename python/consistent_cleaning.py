#!/usr/bin/env python
# -*- coding: utf-8 -*-

# scprit for cleaning inconsistent flow, speed , occupancy

import matplotlib.pyplot as plt
import pandas as pd
# clean_test = pd.read_csv('/home/datascience/Desktop/cleaning_test_06_09.csv',
clean_test = pd.read_csv('/home/datascience/Desktop/cleaning-0611.csv',
names=['lane_id','measurement_start','speed','flow','occupancy','quality'])

# According to traffic flow theory
#         v = c * q / o            (1)
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
# so for sedans c is about 37, and for truck, c is about 104.17

# first clean negative speed and flow
negative_speed_flow = ((clean_test['speed'] < 0) | 
                       (clean_test['flow'] < 0))
clean_test = clean_test[~negative_speed_flow]

# if speed is greater than 0, flow should be greater than 0
abnormal_zero_flow = ((clean_test['flow'] == 0) & 
                      (clean_test['speed'] > 0))
clean_test = clean_test[~abnormal_zero_flow]

# if both speed and flow are zero, occupancy shuold be 0(not car at all) or very high(jam)
jam_occupancy_threshold = 90
abnormal_zero_speed_flow = ((clean_test['speed'] == 0) & 
                            (clean_test['flow'] == 0) & 
                            (clean_test['occupancy'] != 0) & 
                            (clean_test['occupancy'] < jam_occupancy_threshold))
clean_test = clean_test[~abnormal_zero_speed_flow]

# find abnormal speed, if there is no jam, when the speed is slow, flow should be small
# assume length of loop detector is 5 feet, factor is 17.6
abnormal_speed = ((clean_test['speed'] < clean_test['flow'] / 17.6) & 
                  (clean_test['occupancy'] < jam_occupancy_threshold))
clean_test = clean_test[~abnormal_speed]

# find abnormal flow, if flow is large, speed should be high
# assume length of car is 4.5m, factor is 0.434
abnormal_flow = (clean_test['flow'] * 0.434 > clean_test['speed'])
clean_test = clean_test[~abnormal_flow]

# filter positive flow ,speed, occupancy 
positive_measure = (clean_test['flow'] > 0) & (clean_test['speed'] > 0) & (clean_test['occupancy'] > 0)

# add new series 'coefficient' to store c, the constant mentioned above
clean_test['coefficient'] = 0
clean_test.loc[positive_measure, ['coefficient']] = clean_test['speed'] * clean_test['occupancy'] / clean_test['flow']

coefficient_threshold = 500
abnormal_coefficient = (((clean_test['coefficient'] > 0) & (clean_test['coefficient'] < 1)) 
                        | (clean_test['coefficient'] > coefficient_threshold))

clean_test = clean_test[~abnormal_coefficient]

# clean_test[['speed','flow','occupancy', 'coefficient']].describe()
#                 speed            flow       occupancy     coefficient
# count  1324910.000000  1324910.000000  1324910.000000  1324910.000000
# mean        51.563472       13.400633        7.455853       21.773113
# std         24.561942       10.554616       11.576778       17.973971
# min          0.000000        0.000000        0.000000        0.000000
# 25%         44.000000        4.000000        2.000000       18.285714
# 50%         61.000000       12.000000        5.000000       23.333333
# 75%         67.000000       21.000000        9.000000       25.384615
# max        147.000000      107.000000      221.000000      500.000000

