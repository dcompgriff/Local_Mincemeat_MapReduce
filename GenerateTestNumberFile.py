# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 17:57:39 2016

@author: DAN
"""

import random
import sys

dataString = ""
for i in range(0, 1000000):
    if i % 100 == 0:
        print("At num: " + str(i))
    dataString += str(random.random() * (100000)) + '\n'

file = open(sys.argv[1], 'w')
file.write(dataString)
file.close()    
