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

c.parsedSplitandExport(datapath=crsp_datapath, outputdir='', datecolname='date', totalrows=17614314, nrows=10000)

# part II: merge with splist and add sp indicator
# initialization
s = sp.sp(beginyear, endyear, month, file='sp')

sp_data = pd.read_csv(sp_datapath) # read in data
s.extractym(data=sp_data, extractcolname='start', newcolname='sm') # extract month
s.extractym(data=sp_data, extractcolname='ending', newcolname='em') # extract month

# to be tested
for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			c.readdata(y=y, m=m)
			c.mergedata(data=s.TimeIntervalIndexing(y, m), y=y, m=m, how='left', key='PERMNO')
			c.addsp(y=y, m=m)
			print('columns to drop: ', list(c.returndata(y, m).columns)[0:2]+list(c.returndata(y, m).columns)[-5:-1])
			c.dropcol(col=list(c.returndata(y, m).columns)[0:2]+list(c.returndata(y, m).columns)[-5:-1], y=y, m=m)
			c.checkspdup(y=y, m=m)
			c.export(option='w', y=y, m=m, header=True)
	else:
		print('y: ', y)
		c.readdata(y=y, m=month)
		c.mergedata(data=s.TimeIntervalIndexing(y, month), y=y, m=month, how='left', key='PERMNO')
		c.addsp(y=y, m=month)
		print('columns to drop: ', list(c.returndata(y, m).columns)[0:2]+list(c.returndata(y, m).columns)[-5:-1])
		c.dropcol(col=list(c.returndata(y, month).columns)[0:2]+list(c.returndata(y, month).columns)[-5:-1], y=y, m=month)
		c.checkspdup(y=y, m=month)
		c.export(option='w', y=y, m=month, header=True)

# part III: merge with master
# initialization
mst = master.master(beginyear, endyear, month, file='master')

# to be tested
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
		mst.add8CUSIP(y=y, m=month, newcolname='CUSIP_') # add eight-digit cusip to master data
		c.readdata(y=y, m=month)
		c.mergedata(data=mst.returndata(y, month), y=y, m=month, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
		c.export(option='a', y=y, m=month, header=True, file='combined_master_')

# print head and shape of data
# c.print(2008,2)