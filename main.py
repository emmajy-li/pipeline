import pandas as pd
import numpy as np
import time
import csv
import crsp
import master

# input
beginyear = 2008
endyear = 2018
month = 0 # if all month then 0
datapath_crsp = 'crsp_10yr.csv'
datapath_sp = 'dsp500list.csv'

# part I: split crps and merge with splist
# initialization
c = crsp.crsp(beginyear, endyear, month, 'crsp')

# read in and split
# total # of rows: 17614314
for n in range(0,10):
	b = time.time()
	data = pd.read_csv(datapath_crsp, skiprows=range(1,n*10000), nrows=10000)
	c.splitdata(data, append = False)
	if n == 0:
	    c.exportall('a', True) # export csv with header
	else:
		c.exportall('a', False) # export csv without header
	e = time.time()
	if n%100 ==0:
		print(' '.join(['processing time' ,'{y}: {m}']).format(y=n,m=e-b))

# merge with splist and add sp indicator
sp = pd.read_csv(datapath_sp)
sp['sm']=(sp['start']/100).astype(int)
sp['em']=(sp['ending']/100).astype(int)

for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			sp_ = sp.loc[(sp['sm'] <= y*100+m) & (sp['em'] >= y*100+m)] 
			sp_ = sp_.astype(object)
			c.readdata(y, m)
			c.mergedata(sp_, y, m, how='left', key='PERMNO')
			c.addsp(y, m)
			c.dropcol(list(c.returndata(y, m).columns)[0:2]+list(c.returndata(y, m).columns)[-5:-1], y, m)
			c.checkdup(y, m)
			c.export('w', y, m, True)
	else:
		print('y: ', y)
		sp_ = sp.loc[(sp['sm'] <= y*100+month) & (sp['em'] >= y*100+month)] 
		sp_ = sp_.astype(object)
		c.readdata(y, month)
		c.mergedata(sp_, y, month, how='left', key='PERMNO')
		c.addsp(y, month)
		c.dropcol(list(c.returndata(y, month).columns)[0:2]+list(c.returndata(y, month).columns)[-5:-1], y, month)
		c.checkdup(y, month)
		c.export('w', y, month, True)

# part II: merge with master
# initialization
mst = master.master(beginyear, endyear, month, 'master')

# merge with master on eight digit cusip
for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			mst.readdata(y, m)
			master_data = mst.returndata(y, m)
			master_data['CUSIP_'] = [value[:8] for value in master_data['CUSIP']]
			c.readdata(y, m)
			c.mergedata(master_data, y, m, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
			c.export('a', y, m, True, 'combined_master_')
	else:
		print("y: ", y)
		mst.readdata(y, month)
		master_data = mst.returndata(y, month)
		master_data['CUSIP_'] = [value[:8] for value in master_data['CUSIP']]
		c.readdata(y, month)
		c.mergedata(master_data, y, month, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
		c.export('a', y, month, True, 'combined_master_')

# print head and shape of data
# c.print(2008,2)