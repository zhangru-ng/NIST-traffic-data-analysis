#!/usr/bin/python3
import csv
import re

with open('new_detector_lane_inventory.csv', 'w') as outfile, open('detector_lane_inventory.csv', 'r') as inventory:
    reader = csv.reader(inventory)
    writer = csv.writer(outfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
    for row in reader:
        new_row = []
        for item in [re.sub('\n *', '', x) for x in row]:
            try:
                float(item)
                new_row.append(item)
            except ValueError:
                val = item if item == '' else '"' + item + '"'
                new_row.append(val)
        outfile.write(','.join(new_row) + '\n')

