# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 10:43:36 2016

@author: DAN
"""
#!/usr/bin/env python
import mincemeat
import sys
import numpy as np


if len(sys.argv) <= 1:
    sys.exit("No input file! Exiting.")    
    
# Get the file path passed in and set it as the data source.
file = open(sys.argv[1], 'r')
data = list(file)
file.close()
data = map(lambda item: item.strip('\n'), data)
data = map(lambda item: float(item), data)
#The data must be enumerated in a dictionary, with integer keys so that the 
#mincemeat code works. This is bc mincemeat expects inputs with an integer 
#as key.
datasource = dict(enumerate(data))

#NOTE!: Potentially combine data into larger data chunks.

'''
Yield a count and sum for the average and stddev calc.
TODO: Return a sumsquared key for the stddev calc.
'''
def mapfn(k, v):
    keys = ['Count', 'Sum', 'Sumsq']
    for key in keys:
        #Yield an amount for each key.
        if key == 'Count':
            yield key, 1
        elif key == 'Sum':
            yield key, v
        elif key == 'Sumsq':
            yield key, v**2


def reducefn(k, vs):
    result = sum(vs)
    return result
    
    
def collectfn(k, vs):
    result = sum(vs)
    return result

# Set up the "Server", which is really the code to run on the mapreduce workers or "Client".
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn
s.collectfn = collectfn

# Run the mapreduce job, and store the result in "results".
results = s.run_server(password="changeme")

# Do more work here to calculate the final outputs. 
Sum = results['Sum']
Count = results['Count']
Sumsq = results['Sumsq']

# Calculate the stddev. These calculations were derived by unwinding the equation for standard deviation.
# The base equation is stddev = sqrt( (Sumsq) + (-2/Count)*(Sum*Sum) + (Count * ((Sum/Count)**2))  )
Stddev = ((1.0/Count) * ( (Sumsq) + ((-2.0/Count)*(Sum**2)) + (Count*((float(Sum)/Count)**2)) ))**.5

print('Count: ' + str(Count))
print('Sum: ' + str(Sum))
print('Std.dev: ' + str(Stddev))




