# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 22:53:43 2016

@author: DAN
"""

#!/usr/bin/env python
import mincemeat
import time
import hashlib
    

#Generate a list of numbers from 2 to 1 million.
data = range(2, 10000000)
data = map(lambda item: str(item), data)

#The data must be enumerated in a dictionary, with integer keys so that the 
#mincemeat code works. This is bc mincemeat expects inputs with an integer 
#as key.
datasource = dict(enumerate(data))


def mapfn(k, v):    
    '''
    Determine if a number is prime, and yield it if true.
    '''    
    #Yield an amount for each key.
    for value in v:
        yield value, 1


def reducefn(k, vs):
    return vs

#Set up the "Server", which is really the code to run on the mapreduce workers or "Client".
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

# Run the mapreduce job, and store the result in "results".
startTime = time.time()
#Generate all numbers from 2 to 10,000,000 that are prime palindromes.
results = s.run_server(password="changeme")
endTime = time.time()
fullTime = (endTime - startTime) / 60.0
print("Time to run code: " + str(fullTime))
print("Results: ")
print(str(results))