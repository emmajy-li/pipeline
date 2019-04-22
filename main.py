import pandas as pd
import numpy as np
import csv

import sp
import crsp
import master

# input
beginyear = 2009
endyear = 2010
month = 2 # if all month then 0

crsp_datapath = 'crsp_10yr.csv'
sp_datapath = 'dsp500list.csv'

# part I: split crps
# initialization
c = crsp.crsp(beginyear, endyear, month, file='crsp')

for n in range(0,10): # total # of rows: 17614314
	crsp_data = pd.read_csv(crsp_datapath, skiprows=range(1,n*10000), nrows=10000) # read in data
	c.splitdata(data=data, append=False) # split data
	if n == 0:
	    c.exportall(option='a', header=True) # export data with header
	else:
		c.exportall(option='a', header=False) # export data without header
	if n%100 ==0:
		print('n = ', n) 

# part II: merge with splist and add sp indicator
# initialization
s = sp.sp(beginyear, endyear, month, file='sp')

sp_data = pd.read_csv(sp_datapath) # read in data
s.extractym(data=sp_data, extractcolname='start', newcolname='sm') # extract month
s.extractym(data=sp_data, extractcolname='ending', newcolname='em') # extract month

for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			c.readdata(y=y, m=m)
			c.mergedata(data=s.TimeIntervalIndexing(y, m), y=y, m=m, how='left', key='PERMNO')
			c.addsp(y=y, m=m)
			c.dropcol(col=list(c.returndata(y, m).columns)[0:2]+list(c.returndata(y, m).columns)[-5:-1], y=y, m=m)
			c.checkspdup(y=y, m=m)
			c.export(option='w', y=y, m=m, header=True)
	else:
		print('y: ', y)
		c.readdata(y=y, m=month)
		c.mergedata(data=s.TimeIntervalIndexing(y, month), y=y, m=month, how='left', key='PERMNO')
		c.addsp(y=y, m=month)
		c.dropcol(col=list(c.returndata(y, month).columns)[0:2]+list(c.returndata(y, month).columns)[-5:-1], y=y, m=month)
		c.checkspdup(y=y, m=month)
		c.export(option='w', y=y, m=month, header=True)

# part III: merge with master
# initialization
mst = master.master(beginyear, endyear, month, file='master')

for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			mst.readdata(y=y, m=m)
			mst.add8CUSIP(y=y, m=m, newcolname='CUSIP_') # add eight digit cusip to master data
			c.readdata(y=y, m=m)
			c.mergedata(data=mst.returndata(y, m), y=y, m=m, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
			c.export(option='a', y=y, m=m, header=True, file='combined_master_')
	else:
		print("y: ", y)
		mst.readdata(y=y, m=month)
		mst.add8CUSIP(y=y, m=month, newcolname='CUSIP_') # add eight digit cusip to master data
		c.readdata(y=y, m=month)
		c.mergedata(data=mst.returndata(y, month), y=y, m=month, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
		c.export(option='a', y=y, m=month, header=True, file='combined_master_')

# print head and shape of data
# c.print(2008,2)