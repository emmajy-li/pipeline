import pandas as pd
import numpy as np
import csv
import sp
import crsp
import master

# input
beginyear = 2008
endyear = 2017
month = 0 # if all month then 0

crsp10yr_inpath = '/Volumes/FD-CW/crsp_10yr.csv'
sp_inpath = '/Volumes/FD-CW/dsp500list.csv'
crsp_outpath = '/Volumes/FD-CW/crsp/'
crsp_inpath = ''
master_inpath = ''

# crsp_inpath = 'crsp_10yr.csv'
# sp_inpath = 'dsp500list.csv'
# crsp_outpath = ''
# crsp_inpath = ''
# master_inpath = ''

# part I: split crps
# initialization
c = crsp.crsp(beginyear, endyear, month, file='crsp')

c.parsedSplitandExport(datapath=crsp10yr_inpath, outputdir=crsp_outpath, datecolname='date', totalrows=17614314, nrows=10000, timer=True)

# part II: merge with splist and add sp indicator
# initialization
s = sp.sp(beginyear, endyear, month, file='sp')

sp_data = pd.read_csv(sp_inpath) # read in data
s.extractym(data=sp_data, extractcolname='start', newcolname='sm') # extract month
s.extractym(data=sp_data, extractcolname='ending', newcolname='em') # extract month

for y in range(beginyear, endyear+1):
		if month==0:
				for m in range(1,13):
						c.readdata(datapath=crsp_inpath, y=y, m=month)
						c.mergedata(data=s.TimeIntervalIndexing(y, m), y=y, m=m, how='left', key='PERMNO')
						c.addsp(y=y, m=m)
						c.dropcol(col=list(c.returndata(y, m).columns)[-5:-1], y=y, m=m)
						c.checkspdup(y=y, m=m)
						c.export(option='w', y=y, m=m, header=True)
		else:
				m = month
				print('y: ', y)
				c.readdata(datapath=crsp_inpath, y=y, m=month)
				c.mergedata(data=s.TimeIntervalIndexing(y, m), y=y, m=m, how='left', key='PERMNO')
				c.addsp(y=y, m=m)
				c.dropcol(col=list(c.returndata(y, m).columns)[-5:-1], y=y, m=m)
				c.checkspdup(y=y, m=m)
				c.export(option='w', y=y, m=m, header=True)

# part III: merge with master
# initialization
mst = master.master(beginyear, endyear, month, file='master')

for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			mst.readdata(datapath=master_inpath, y=y, m=m)
			mst.add8CUSIP(y=y, m=m, newcolname='CUSIP_8') # add eight digit cusip to master data
			c.readdata(datapath=crsp_inpath, y=y, m=m)
			c.mergedata(data=mst.returndata(y, m), y=y, m=m, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_8', 'DATE'])
			c.export(option='a', y=y, m=m, header=True, file='combined_master_')
	else:
		m = month
		mst.readdata(datapath=master_inpath, y=y, m=m)
		mst.add8CUSIP(y=y, m=m, newcolname='CUSIP_8') # add eight digit cusip to master data
		c.readdata(datapath=crsp_inpath, y=y, m=m)
		c.mergedata(data=mst.returndata(y, m), y=y, m=m, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_8', 'DATE'])
		c.export(option='a', y=y, m=m, header=True, file='combined_master_')

# print head and shape of data
# c.print(2008,2)