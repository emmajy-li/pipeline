import pandas as pd
import crsp

# input
beginyear = 2008
endyear = 2018
month = 2 # if all month then 0
datapath_crsp = 'crsp_10yr.csv'
datapath_sp = 'sp500list_201502.csv'

# initialization
crsp1 = crsp.crsp(beginyear, endyear, month)

# read in and split
# total # of rows: 17614314
for n in range(0,1762):
	if n==0:
		data = pd.read_csv(datapath, nrows=10000)
		crsp1.split(data)
	else:
		data = pd.read_csv(datapath, skiprows=range(1,n*10000), nrows=10000)
		crsp1.split(data)

# merge with splist and add sp indicator
crsp1.merge(pd.read_csv(datapath_sp), 'left', 'PERMNO')
crsp1.addsp()

# print head and shape of data
crsp1.print(2008,2)

# export csv
crsp1.export()