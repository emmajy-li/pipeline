from wrdsdata import wrdsdata

class master(wrdsdata):
	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for master class.

		Parameters: 
           startyear (int): The startyear of the desired output data
           endyear (int): The endyear of the desired output data
           month (int): The month of the desired output data; 0 means every month
           file (str): the file name of the desired output data
		"""
		wrdsdata.__init__(self, startyear, endyear, month, file)
		pass

	def add8CUSIP(self, y, m, newcolname):
		filename=self.getfilename(y, m)
		self.d[filename][newcolname] = [value[:8] for value in self.d[filename]['CUSIP']]
		pass

	
