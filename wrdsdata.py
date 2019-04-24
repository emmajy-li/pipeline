import pandas as pd 
import numpy as np
import time

class wrdsdata:

	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for wrdsdata class.

		Args: 
           startyear (int): The startyear of the desired output data. 
           endyear (int): The endyear of the desired output data.
           month (int): The month of the desired output data; 0 means every month.
           file (str): the file name of the desired output data.

        Returns:
        	no returns.
		"""
		self.start=startyear
		self.end=endyear
		self.month=month
		self.file = file
		self.d={}
		self._initDataFrame()
		pass

	def _getfilename(self, y, m):
		"""

		The private function to get name of the stored data with specific timestamp, 
		defined by year and month.

		Args:
			y (int): The year of the data.
			m (int): The month of the data.

		Returns:
			filename (str): The filename combining file type and year month.
		"""
		if m < 10:
			filename='_'.join([self.file,'{y}0{m}']).format(y=y,m=m)
		else:
			filename='_'.join([self.file,'{y}{m}']).format(y=y,m=m)
		return(filename)

	def _initDataFrame(self):
		"""
		The private function to initialize dataframes in the dictionary d{} to store data.

		Args:
			no args.

		Return:
			function changes data stored in dictionary, but produces no returns.	
		"""
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename = self._getfilename(y, m)
					self.d[filename] = pd.DataFrame()
			else:
				m = self.month
				filename = self._getfilename(y, m)
				self.d[filename] = pd.DataFrame()
		pass
	
	def readdata(self, datadir, y, m):
		"""
		The function to read in and store the data with specific timestamp, 
		defined by year and month.

		Args: 
			datadir (str): The directory of the data to be read in.
			y (int): The year of the data.
			m (int): The month of the data.

		Return:
			function changes data stored in dictionary, but produces no returns.	
		"""
		filename=self._getfilename(y, m)
		self.d[filename] = pd.DataFrame()
		self.d[filename] = pd.read_csv('.'.join([datapath+filename, 'csv']))
		pass

	def returndata(self, y, m):
		"""
		The function to return the stored data with specific timestamp, 
		defined by year and month.

		Args:
			y (int): The year of the data.
			m (int): The month of the data.

		Returns:
			self.d[filename] (DataFrame): The requested data from dictionary. key = filename.
		"""
		filename = self._getfilename(y, m)
		return(self.d[filename])

	def splitdata(self, data, datecolname):
		"""
		The function to split data into monthly data and store them in the dictionary. 

		Note: 
			data can be accessed using method .returndata(y=,m=);
			data can be printed using method .print(y=,m=).

		Args:
			data (DataFrame): The data to be split.
			datecolname (str): The column name of the column that sotres date.

		Return:
			function changes data stored in dictionary, but produces no returns.	

		"""
		dt = data.applymap(str)
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self._getfilename(y, m)
					self.d[filename]=dt.loc[dt[datecolname].str.startswith(filename[len(filename)-6:])]
			else:
				m = self.month
				filename=self._getfilename(y, m)
				self.d[filename]=dt.loc[dt[datecolname].str.startswith(filename[len(filename)-6:])]
		pass

	def batchSplitandExport(self, datapath, outputdir, datecolname, totalrows, batchsize, timer):
		"""
		The function to split a big dataset and export it as monthly data.

		The function takes on dataset that cannot be read in at one time,
		reads in part of the dataset, split it into monthly data and export as csv.

		Note:
			totalrows of a big dataset can be gotten without open the file by using:
			
			For Linux/Unix:
				wc -l filename
			For Windows:
				find /c /v "A String that is extremely unlikely to occur" filename

			Ref: https://stackoverflow.com/questions/32913151

		Args: 
			datapath (str): The path of the dataset to be split.
			outputdir (str): The directory of the output csv file.
			datecolname (str): The column name of the dataset with date input.
			totalrows (int): The number of rows of the dataset.
			batchsize (int): The number of rows to read in each time.
			timer (bool): The report of processing time of one batch for every one hundred batches.

		Returns:
			funtion outputs csv files, but has no return.

		"""
		for n in range(0, int(totalrows/batchsize)+1): 
			if timer == True:
				b = time.time()
			data = pd.read_csv(datapath, skiprows=range(1,n*batchsize), nrows=batchsize) # read in data
			self.splitdata(data=data, datecolname=datecolname) # split data # to change
			if n == 0:
				self.exportall(option='w',outputdir=outputdir, header=True) # export data with header
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

	def bulkSplitandExport(self, datapath, datecolname, timer):
		"""
		The function to split a big dataset and export it as monthly data, at one time.

		Args:
			datapath (str): The path of the dataset to be split.
			datecolname (str): The column name of the dataset with date input.
			timer (bool): The report of processing time of one batch for every one hundred batches.

		Return:
			no returns.
		"""

		if timer == True:
			b = time.time()
		data = pd.read_csv(datapath)
		self.splitdata(data=data, datecolname=datecolname)
		self.exportall(option='a', header=True)
		if timer == True:
			e = time.time()
			print('processing time is {c} seconds. Current time is {d}.'.format(c=e-b, d=e))

		pass
			
	def mergedata(self, data, y, m, how='left', key=None, left_on=None, right_on=None):
		"""
		The function for merging data to the stored data with specific timestamp, 
		defined by year and month.

		Args:
			data (DataFrame): data to merge with.
			y (int): The year of the data.
			m (int): The month of the data.
			how (str): Ref.
			key (str) Ref.
			left_on (str) = Ref.
			right_on (str) = Ref.
			Ref: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html
			
		Return:
			function changes data stored in dictionary, but produces no returns.	
		"""
		filename = self._getfilename(y, m)
		self.d[filename]=self.d[filename].merge(right=data, how=how, on=key, left_on=left_on, right_on=right_on)
		pass

	def addsp(self, y, m):
		"""
		The function to add s&p500 indicator in the stored data with specific timestamp, 
		defined by year and month.
		
		Args:
			y (int): The year of the data.
			m (int): The month of the data.
		
		Return:
			function changes data stored in dictionary, but produces no returns.	
		"""
		filename=self._getfilename(y, m)
		cond = (self.d[filename]['date'] >= self.d[filename]['start']) & (self.d[filename]['date']<=self.d[filename]['ending'])
		self.d[filename]['sp'] = np.where(cond, 1, 0)
		pass
	
	def dropcol(self, col, y, m):
		"""
		The function to drop certain columns in the stored data with specific timestamp, 
		defined by year and month.
		
		Args:
			col (list of str): The list of names of the columns to be dropped.
			y (int): The year of the data.
			m (int): The month of the data.
		
		Return:
			function changes data stored in dictionary, but produces no returns.	
		"""
		filename=self._getfilename(y, m)
		self.d[filename] = self.d[filename].drop(col, axis = 1)
		pass

	def changecolname(self, old, new, y, m):
		"""
		The function to change certain column name in the stored data with specific timestamp, 
		defined by year and month.

		Args:
			old (str): The name of the column to be changed
			new (str): The name of the column to be placed
			y (int): The year of the data.
			m (int): The month of the data.
		
		Return:
			no returns.	
		"""
		filename=self._getfilename(y, m)
		self.d[filename].rename(columns={old:new}, inplace=True)
		pass

	def checkspdup(self, y, m):
		"""
		The function to check duplication without s&p500 indicator in the stored data with specific timestamp, 
		defined by year and month.

		Args:
			y (int): The year of the data.
			m (int): The month of the data.
		
		Return:
			no returns.	
		"""
		filename=self._getfilename(y, m)
		if self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].empty:
			print("No Duplication")
		else:
			print('Special Attention is Needed for Duplicates! \n')
			print(self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])])
		pass

	def exportall(self, option, outputdir, header):
		"""
		
		Return:
			no returns.	
		"""
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self._getfilename(y, m)
					with open(outputdir+filename+'.csv', option) as csvFile:
						self.d[filename].to_csv(csvFile, header=header, index=False)
			else:
				m = self.month
				filename=self._getfilename(y, m)
				with open(outputdir+filename+'.csv', option) as csvFile:
					self.d[filename].to_csv(csvFile, header=header, index=False)
		pass

	def export(self, option, y, m, outputdir, header, file=None):
		"""

		Args:
			y (int): The year of the data.
			m (int): The month of the data.
		
		Return:
			no returns.	
		"""
		filename=self._getfilename(y, m)
		if file != None:
			with open(outputdir+file + filename +'.csv', option) as csvFile:
				self.d[filename].to_csv(csvFile, header=header, index=False)
		else:
			with open(outputdir+filename +'.csv', option) as csvFile:
				self.d[filename].to_csv(csvFile, header=header, index=False)
		pass

	def print(self, y, m):
		"""
		The function to print the head and shape of a spefic data stored in dictionary.

		Args:
			y (int): The year of the data.
			m (int): The month of the data.
		
		Return:
			no returns.	
		"""
		print("shape: ",self.returndata(y, m).shape)
		print("head: ",self.returndata(y, m).head())
		pass
