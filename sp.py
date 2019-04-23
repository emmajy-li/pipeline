from wrdsdata import wrdsdata

class sp(wrdsdata):
	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for sp class.

		Parameters: 
           startyear (int): The startyear of the desired output data
           endyear (int): The endyear of the desired output data
           month (int): The month of the desired output data; 0 means every month
           file (str): the file name of the desired output data
		"""
		wrdsdata.__init__(self, startyear, endyear, month, file)
		pass

	def extractym(self, data, extractcolname, newcolname):
		self.spdata = data
		self.spdata[newcolname] = (self.spdata[extractcolname]/100).astype(int)
		pass

	def TimeIntervalIndexing(self, y, m):
		sp_tomerge = self.spdata.loc[(self.spdata['sm'] <= y*100+m) & (self.spdata['em'] >= y*100+m)]
		return(sp_tomerge)

	def returndata(self):
		return(self.spdata)
