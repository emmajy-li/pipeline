import pandas as pd
import crsp

# input
beginyear = 2008
endyear = 2018
month = 0 # if all month then 0
master_prefix = 'master' # the prefix of master filename
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

# merge with master on eight digit cusip
for y in range(self.start, self.end+1):
	if self.month==0:
		for m in range(1,13):
			master = pd.read_csv('_'.join([master_prefix, '{y}0{m}']).format(y=y,m=m))
			master['CUSIP_'] = [value[:8] for value in master['CUSIP']]
			c.onemerge(master, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'], y, m)
	else:
		master = pd.read_csv('_'.join(['master', '{y}{m}']).format(y=y,m=m))
		master['CUSIP_'] = [value[:8] for value in master['CUSIP']]
		c.onemerge(master, 'outer', left_on=['CUSIP','date'], right_on=['CUSIP_', 'DATE'], y, m)

# print head and shape of data
c.print(2008,2)

# export csv
c.export()
