from wrdsdata import wrdsdata

class master(wrdsdata):
	"""
	This is a class inherited from wrdsdata class to process master data.

	Args: 
		startyear (int): The startyear of the desired output data.
		endyear (int): The endyear of the desired output data.
		month (int): The month of the desired output data; 0 means every month.
		file (str): the file name of the desired output data.
	"""

	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for master class.

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

	def add8CUSIP(self, y, m, newcolname, outputdir):
		"""
		The function to add 8-digit cusip based on 9-digit cusip in the dataset.

		Args:
			y (int): The year of the data.
			m (int): The month of the data.
			newcolname (str): The name of the newly created column of 8-digit cusip.

		Returns:
			no returns.
		"""
		filename=self._getfilename(y, m)
		cusip_nan = self.d[filename][self.d[filename]['CUSIP'].isna()]
		if !cusip_nan.empty:
			with open(outputdir + filename +'_CUSIP_NaN.csv', 'w') as csvFile:
				cusip_nan.to_csv(csvFile, header=True, index=False)
		self.d[filename] = self.d[filename].dropna(subset=['CUSIP'])
		self.d[filename][newcolname] = [value[:8] for value in self.d[filename]['CUSIP']]
		pass