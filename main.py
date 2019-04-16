import pandas as pd
import numpy as np
import csv
import crsp
import master

# input
beginyear = 2008
endyear = 2018
month = 0 # if all month then 0
datapath_crsp = 'crsp_10yr.csv'
datapath_sp = 'sp500list_201502.csv'

# initialization
c = crsp.crsp(beginyear, endyear, month, 'crsp')

# read in and split
# total # of rows: 17614314
for n in range(0,1762):
		data = pd.read_csv(datapath_crsp, skiprows=range(1,n*10000), nrows=10000)
		c.split(data)

# merge with splist and 
c.allmerge(pd.read_csv(datapath_sp), 'left', 'PERMNO')

# add sp indicator
c.addsp()

# initialization
mst = master.master(beginyear, endyear, month, 'master')

# merge with master on eight digit cusip
for y in range(beginyear, endyear+1):
	if month==0:
		for m in range(1,13):
			mst.readdata(y, m)
			master_data = mst.returndata(y, m)
			master_data['CUSIP_'] = [value[:8] for value in master_data['CUSIP']]
			c.onemerge(master_data, y, m, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])
	else:
		mst.readdata(y, m)
		master_data = mst.returndata(y, m)
		master_data['CUSIP_'] = [value[:8] for value in master_data['CUSIP']]
		c.onemerge(master_data, y, month, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'])

# print head and shape of data
c.print(2008,2)

# export csv
c.export()

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