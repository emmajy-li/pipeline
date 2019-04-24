import pandas as pd 
import numpy as np
import time

class wrdsdata:

	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for wrdsdata class.

		Parameters: 
           startyear (int): The startyear of the desired output data
           endyear (int): The endyear of the desired output data
           month (int): The month of the desired output data; 0 means every month
           file (str): the file name of the desired output data
		"""
		self.start=startyear
		self.end=endyear
		self.month=month
		self.file = file
		self.d={}
		self.initDataFrame()
		pass

	def __getfilename__(self, y, m):
		"""

		"""
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
				m = self.month
				filename = self.getfilename(y, m)
				self.d[filename] = pd.DataFrame()
		pass
	
	def readdata(self, datapath, y, m):
		filename=self.getfilename(y, m)
		self.d[filename] = pd.DataFrame()
		self.d[filename] = pd.read_csv('.'.join([datapath+filename, 'csv']))
		pass

	def returndata(self, y, m):
		filename = self.getfilename(y, m)
		return(self.d[filename])

	def splitdata(self, data, datecolname, append):
		dt = data.applymap(str)
		self.append = append
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					if self.append == True:
						self.d[filename]=self.d[filename].append(dt.loc[dt[datecolname].str.startswith(filename[len(filename)-6:])])
					else:
						self.d[filename]=dt.loc[dt[datecolname].str.startswith(filename[len(filename)-6:])]
			else:
				m = self.month
				filename=self.getfilename(y, m)
				if self.append == True:
					self.d[filename]=self.d[filename].append(dt.loc[dt[datecolname].str.startswith(filename[len(filename)-6:])])
				else:
					self.d[filename]=dt.loc[dt[datecolname].str.startswith(filename[len(filename)-6:])]
		pass

	def parsedSplitandExport(self, datapath, outputdir, datecolname, totalrows, batchsize, timer):
		"""
		The function to split and export a big dataset as monthly data.

		The function takes on dataset that cannot be read in at one time,
		reads in part of the dataset, split it into monthly data and export as csv.

		Parameters: 
			datapath (str): The path of the dataset to be split
			outputdir (str): The directory of the output csv file
			datacolname (str): The column name of the dataset with date input
			totalrows (int): The number of rows of the dataset
			batchsize (int): The number of rows to read in each time
			timer (bool): The report of processing time of one batch for every one hundred batches

		Returns:
			funtion outputs csv files, but has no return.

		"""
		for n in range(0, int(totalrows/batchsize)+1): 
			if timer == True:
				b = time.time()
			data = pd.read_csv(datapath, skiprows=range(1,n*batchsize), nrows=batchsize) # read in data
			self.splitdata(data=data, datecolname=datecolname, append=False) # split data # to change
			if n == 0:
				self.exportall(option='a',outputdir=outputdir, header=True) # export data with header
			else:
				self.exportall(option='a',outputdir=outputdir, header=False) # export data without header
			if timer == True:
				e = time.time()
			if n%100 ==0:
				print('current batch is', n)
				if timer == True:
					print('processing time for {a} rows in No.{b} batch is {c} seconds. Current time is {d}.'\
						.format(a=batchsize, b=n, c=e-b, d=e)) 
			pass

	def bulkSplitandExport(self, data, datecolname):
		self.splitdata(data=data, datecolname=datecolname, append=False)
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
		
	def changecolname(self, old, new, y, m):
		filename=self.getfilename(y, m)
		self.d[filename].rename(columns={old:new}, inplace=True)
		pass

	def checkspdup(self, y, m):
		filename=self.getfilename(y, m)
		if self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].empty:
			print("No Duplication")
		else:
			print('Special Attention is Needed for Duplicates! \n')
			print(self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])])
		pass

	def exportall(self, option, outputdir, header):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					with open(outputdir+filename+'.csv', option) as csvFile:
						self.d[filename].to_csv(csvFile, header=header, index=False)
			else:
				m = self.month
				filename=self.getfilename(y, m)
				with open(outputdir+filename+'.csv', option) as csvFile:
					self.d[filename].to_csv(csvFile, header=header, index=False)
		pass

	def export(self, option, y, m, outputdir, header, file=None):
		filename=self.getfilename(y, m)
		if file != None:
			with open(outputdir+file + filename +'.csv', option) as csvFile:
				self.d[filename].to_csv(csvFile, header=header, index=False)
		else:
			with open(outputdir+filename +'.csv', option) as csvFile:
				self.d[filename].to_csv(csvFile, header=header, index=False)
		pass

	def print(self, y, m):
		print("shape: ",self.returndata(y, m).shape)
		print("head: ",self.returndata(y, m).head())
		pass
