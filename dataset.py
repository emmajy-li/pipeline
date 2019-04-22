import pandas as pd 
import numpy as np

class dataset:

	def __init__(self, startyear, endyear, month, file):
		self.start=startyear
		self.end=endyear
		self.month=month
		self.file = file
		self.d={}
		self.initDataFrame()
		pass

	def getfilename(self, y, m):
		if m < 10:
			filename='_'.join([self.file,'{y}0{m}']).format(y=y,m=m)
		else:
			filename='_'.join([self.file,'{y}{m}']).format(y=y,m=m)
		return(filename)

	def initDataFrame(self):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename = self.getfilename(y, m)
					self.d[filename] = pd.DataFrame()
			else:
				filename = self.getfilename(y, self.month)
				self.d[filename] = pd.DataFrame()
		pass
	
	def readdata(self, y, m):
		filename=self.getfilename(y, m)
		self.d[filename] = pd.DataFrame()
		self.d[filename] = pd.read_csv('.'.join([filename, 'csv']))
		pass

	def returndata(self, y, m):
		filename = self.getfilename(y, m)
		return(self.d[filename])

	def splitdata(self, data, append):
		dt = data.applymap(str)
		self.append = append
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					if self.append == True:
						self.d[filename]=self.d[filename].append(dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])])
					else:
						self.d[filename]=dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])]
			else:
				filename = self.getfilename(y, self.month)
				if self.append == True:
					self.d[filename]=self.d[filename].append(dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])])
				else:
					self.d[filename]=dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])]
		pass

	def parsedSplitandExport(self, datapath, n):
		for n in range(0,n): 
			data = pd.read_csv(datapath, skiprows=range(1,n*10000), nrows=10000) # read in data
			self.splitdata(data=data, append=False) # split data
			if n == 0:
				self.exportall(option='a', header=True) # export data with header
			else:
				self.exportall(option='a', header=False) # export data without header
			if n%100 ==0:
				print('n = ', n) 
			pass

	def bulkSplitandExport(self, data):
		self.splitdata(data=data, append=False)
		self.exportall(option='a', header=True)
		pass
			
	def mergedata(self, data, y, m, how='left', key=None, left_on=None, right_on=None):
		filename = self.getfilename(y, m)
		self.d[filename]=self.d[filename].merge(right=data, how=how, on=key, left_on=left_on, right_on=right_on)
		pass

	def addsp(self, y, m):
		filename=self.getfilename(y, m)
		cond = (self.d[filename]['date'] >= self.d[filename]['start']) & (self.d[filename]['date']<=self.d[filename]['ending'])
		self.d[filename]['sp'] = np.where(cond, 1, 0)
		pass
	
	def dropcol(self, col, y, m):
		filename=self.getfilename(y, m)
		self.d[filename] = self.d[filename].drop(col, axis = 1)
		pass

	def checkspdup(self, y, m):
		filename=self.getfilename(y, m)
		if self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].empty: # to be tested
			print("No Duplication")
		else: # to be tested
			print('Duplicates', self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].shape)
			print('Pre-Data', self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].shape)
			self.d[filename] = self.d[filename].groupby(list(self.d[filename].columns)[:-1], as_index=False)['sp'].sum()
			print('Post-Data', self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].shape)
			if self.d[filename]['sp'] > 1:
				raise ValueError('sp indicator should not be less than 1!')
		pass

	def exportall(self, option,header):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					with open(filename+'.csv', option) as csvFile:
						self.d[filename].to_csv(csvFile, header=header)
			else:
				filename=self.getfilename(y, self.month)
				with open(filename+'.csv', option) as csvFile:
					self.d[filename].to_csv(csvFile, header=header)
		pass

	def export(self, option, y, m, header, file=None):
		filename=self.getfilename(y, m)
		if file != None:
			with open(file + filename +'.csv', option) as csvFile:
				self.d[filename].to_csv(csvFile, header=header)
		else:
			with open(filename +'.csv', option) as csvFile:
				self.d[filename].to_csv(csvFile, header=header)
		pass

	def print(self, y, m):
		print("shape: ",self.returndata(y, m).shape)
		print("head: ",self.returndata(y, m).head())
		pass

