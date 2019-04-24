from wrdsdata import wrdsdata

class sp(wrdsdata):
	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for sp class.

		Args: 
           startyear (int): The startyear of the desired output data.
           endyear (int): The endyear of the desired output data.
           month (int): The month of the desired output data; 0 means every month.
           file (str): the file name of the desired output data.

        Returns:
        	no returns.
		"""
		wrdsdata.__init__(self, startyear, endyear, month, file)
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

	def TimeIntervalIndexing(self, y, m):
		sp_tomerge = self.spdata.loc[(self.spdata['sm'] <= y*100+m) & (self.spdata['em'] >= y*100+m)]
		return(sp_tomerge)

	def returndata(self):
		return(self.spdata)
