import pandas as pd
import numpy as np
import time
import csv
import crsp
import master

# input
beginyear = 2008
endyear = 2018
month = 2 # if all month then 0
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
	c.split(data, append = False)
	c.export() # export csv
	e = time.time()
	if n%100 ==0:
		print(' '.join(['processing time' ,'{y}: {m}']).format(y=n,m=e-b))

# merge with splist and 
for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			c.readdata(y, m)
			c.onemerge(pd.read_csv(datapath_sp, dtype = object), y, m, how='left', left_on= 'PERMNO',right_on='PERMNO')
			c.addsp(y, m) # add sp indicator
			c.export()
	else:
		c.readdata(y, month)
		c.onemerge(pd.read_csv(datapath_sp, dtype = object), y, month, how='left', left_on= 'PERMNO',right_on='PERMNO')
		c.addsp(y, month) # add sp indicator
		c.export()

# part II: merge with master
# initialization
# mst = master.master(beginyear, endyear, month, 'master')

# # merge with master on eight digit cusip
# for y in range(beginyear, endyear+1):
# 	if month==0:
# 		for m in range(1,13):
# 			mst.readdata(y, m)
# 			master_data = mst.returndata(y, m)
# 			master_data['CUSIP_'] = [value[:8] for value in master_data['CUSIP']]
# 			c.readdata(y, m)
# 			c.onemerge(master_data, y, m, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
# 	else:
# 		mst.readdata(y, m)
# 		master_data = mst.returndata(y, m)
# 		master_data['CUSIP_'] = [value[:8] for value in master_data['CUSIP']]
# 		c.readdata(y, m)
# 		c.onemerge(master_data, y, month, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])

# print head and shape of data
# c.print(2008,2)

# test

# import pandas as pd
# import numpy as np
# import csv
# import crsp

# beginyear = 2011
# endyear = 2011
# month = 2
# master_prefix = 'master_combined'
# datapath_crsp = 'crsp_201102.csv'
# c = crsp.crsp(beginyear, endyear, month, 'crsp')
# c.readdata(beginyear, month)