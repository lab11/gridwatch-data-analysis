#!/usr/bin/env python3

import sys
import os
import csv
import re
from datetime import datetime

core_grid = {}

with open('achimota_grid.csv') as csvfile:
    reader = csv.reader(csvfile)

    first = True
    for row in reader:
        if(first == True):
            first = False
            continue
    
        if row[0] in core_grid:
            print('duplicate')

        substation = re.search('^[A-Z]*',row[4])
        substation = substation.group(0)
        
        #convert start and end time to unix time
        start_time = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')

        end_time = None
        if row[2] == '':
            #just make it an unreasonably large time for easy comparison
            end_time = datetime.strptime("2300-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
        else:
            end_time = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')

        core_grid[row[0]] = (start_time.timestamp(), end_time.timestamp(), row[3], row[4], substation)

#okay now we have a dict of core_ids
#create two loops to find grid distance
grid_distance = []

for core_1, grid_1 in core_grid.items():
    for core_2, grid_2 in core_grid.items():
        same_tx = (grid_1[2] == grid_2[2])
        same_feeder = (grid_1[3] == grid_2[3])
        same_substation = (grid_1[4] == grid_2[4])

        distance = 4
        if same_tx:
            distance = 1
        elif same_feeder:
            distance = 2
        elif same_substation:
            distance = 3

        #find the latest start time and the earliest end time
        #they are only valid between those times
        start_time = None
        end_time = None
        if grid_1[0] >= grid_2[0]:
            start_time = grid_1[0]
        else: 
            start_time = grid_2[0]

        if grid_1[1] <= grid_2[1]:
            end_time = grid_1[1]
        else: 
            end_time = grid_2[1]

        grid_distance.append((core_1, core_2, start_time, end_time, distance))

f = open('grid_distance.csv', 'w+')
f.write('core_1,core_2,logical_grid_distance\n')
for v in grid_distance:
    f.write(v[0] + ',' + str(v[1]) + ',' + str(v[2]) + ',' + str(v[3]) + ',' + str(v[4]) + '\n')

f.close()
