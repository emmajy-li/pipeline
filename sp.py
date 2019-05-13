from wrdsdata import wrdsdata

class sp(wrdsdata):
	"""
	This is a class inherited from wrdsdata class to process s&p500 data, s&p500 list.

	Args: 
           startyear (int): The startyear of the desired output data.
           endyear (int): The endyear of the desired output data.
           month (int): The month of the desired output data; 0 means every month.
           file (str): the file name of the desired output data.
	"""
	
	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for sp class.

		Args: 
           startyear (int): The startyear of the desired output data.
           endyear (int): The endyear of the desired output data.
           month (int): The month of the desired output data; 0 means every month.
           file (str): the file name of the desired output data.
		"""
		wrdsdata.__init__(self, startyear, endyear, month, file)
		pass

	def readdata(self, data):
		"""
		The function for reading in data.

		Args:
			data (DataFrame): The s&p500 or s&p500 list data.

		Returns:
			no returns.
		"""
		self.spdata = data
		pass

	def extractym(self, data, extractcolname, newcolname):
		"""
		The function for creating a year month column by extracting from date column.

		Args:
			data (DataFrame): The data with date column and to create new column.
			extractcolname (str): The name of the column to be extracted from.
			newcolname (str): The name of the column to be created.

		Returns:
			no returns.
		"""
		self.spdata = data
		self.spdata[newcolname] = (self.spdata[extractcolname]/100).astype(int)
		pass

	def TimeIntervalIndexing(self, y, m, intervalstart, intervalend):
		"""
		The function for indexing the dataset based on time interval defined by year month
		timestamps in two columns.

		Args: 
			y (int): The year of the data to be encompassed by the interval.
			m (int): The month of the data to be encompassed by the interval.
			intervalstart (str): The name of the column with the start of the time interval.
			intervalend (str): The name of the column with the end of the time interval.

		Returns:
			newspdata (DataFrame): an indexed dataframe.
		"""
		newspdata = self.spdata.loc[(self.spdata[intervalstart] <= y*100+m) & (self.spdata[intervalend] >= y*100+m)]
		return(newspdata)

	def returndata(self):
		"""
		The function for returning the s&p500 or s&p500 list data read in.

		Args:
			no arguments.

		Returns:
			self.spdata (DataFrame): the s&p500 or s&p500 list data stored in memory.
		"""
		return(self.spdata)
