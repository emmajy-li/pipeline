import pandas as pd
import crsp

# input
beginyear = 2008
endyear = 2018
month = 2 # if all month then 0
datapath = 'crsp_10yr.csv'

# initialization
crspobj = crsp.crsp(beginyear, endyear, month)

# read in and split
# total # of rows: 17614314
for n in range(0,1762):
	if n==0:
		data = pd.read_csv(datapath, nrows=10000)
		crspobj.split(data)
	else:
		data = pd.read_csv(datapath, skiprows=range(1,n*10000), nrows=10000)
		crspobj.split(data)

# print head and shape of data
crspobj.print(2008,2)

# export csv
crspobj.export()
    