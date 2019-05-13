import pandas as pd
import numpy as np
import csv
import sp
import crsp
import master

# input
beginyear = 2017
endyear = 2017
month = 0 # if all month then 0

crsp10yr_inpath = '/Volumes/FD-CW/'
crsp10yr_filename = 'crsp_10yr.csv'
sp_inpath = '/Volumes/FD-CW/'
sp_filename = 'dsp500list.csv'
crsp_outpath = '/Volumes/FD-CW/crsp/'
crsp_inpath = '/Volumes/FD-CW/crsp/'
master_inpath = '~/Dropbox/invariance-cw/data/master/'
master_outpath = '/Volumes/FD-CW/combined/'

# part I: split crps
# initialization
c = crsp.crsp(beginyear, endyear, month, file='crsp')

c.batchSplitandExport(datapath=crsp10yr_inpath+crsp10yr_filename, outputdir=crsp_outpath, datecolname='date', totalrows=17614314, batchsize=10000, timer=True)

# part II: merge with splist and add sp indicator
# initialization
s = sp.sp(beginyear, endyear, month, file='sp')

sp_data = pd.read_csv(sp_inpath+sp_filename) # read in data
s.extractym(data=sp_data, extractcolname='start', newcolname='sm') # extract month
s.extractym(data=sp_data, extractcolname='ending', newcolname='em') # extract month

for y in range(beginyear, endyear+1):
		if month==0:
			for m in range(4,13):
				print('{a}-{b}:'.format(a=y,b=m))
				c.readdata(datadir=crsp_inpath, y=y, m=m)
				c.mergedata(data=s.TimeIntervalIndexing(y, m, 'sm', 'em'), y=y, m=m, how='left', key='PERMNO')
				c.addsp(y=y, m=m)
				c.dropcol(col=list(c.returndata(y, m).columns)[-5:-1], y=y, m=m)
				c.checkspdup(y=y, m=m)
				c.export(option='w', y=y, m=m, header=True)
		else:
			m = month
			print('{a}-{b}:'.format(a=y,b=m))
			c.readdata(datadir=crsp_inpath, y=y, m=m)
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
			print('{a}-{b}:'.format(a=y,b=m))
			mst.readdata(datadir=master_inpath, y=y, m=m)
			mst.add8CUSIP(y=y, m=m, newcolname='CUSIP_8') # add eight digit cusip to master data
			c.readdata(datadir=crsp_inpath, y=y, m=m)
			c.mergedata(data=mst.returndata(y, m), y=y, m=m, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_8', 'DATE'])
			c.dropcol(col=['DATE','CUSIP_8'], y=y, m=m)
			c.changecolname(old='CUSIP_x', new='CUSIP', y=y, m=m)
			c.changecolname(old='CUSIP_y', new='CUSIP_9', y=y, m=m)
			c.export(option='w', y=y, m=m, outputdir=master_outpath, header=True, file='combined_master_')
	else:
		m = month
		print('{a}-{b}:'.format(a=y,b=m))
		mst.readdata(datadir=master_inpath, y=y, m=m)
		mst.add8CUSIP(y=y, m=m, newcolname='CUSIP_8') # add eight digit cusip to master data
		c.readdata(datadir=crsp_inpath, y=y, m=m)
		c.mergedata(data=mst.returndata(y, m), y=y, m=m, how='outer', left_on=['CUSIP','date'], right_on=['CUSIP_8', 'DATE'])
		c.dropcol(col=['DATE','CUSIP_8'], y=y, m=m)
		c.changecolname(old='CUSIP_x', new='CUSIP', y=y, m=m)
		c.changecolname(old='CUSIP_y', new='CUSIP_9', y=y, m=m)
		c.export(option='w', y=y, m=m, outputdir=master_outpath, header=True, file='combined_master_')

# part IV
# print head and shape of data
# c.print(2008,2)