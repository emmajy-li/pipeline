from dataset import dataset

class sp(dataset):
	def __init__(self, startyear, endyear, month, file):
		dataset.__init__(self, startyear, endyear, month, file)
		pass

	def extractym(self, data, extractcolname, newcolname):
		self.spdata = data
		self.spdata[newcolname] = (self.spdata[extractcolname]/100).astype(int)
		pass

	def TimeIntervalIndexing(self, y, m):
		sp_tomerge = self.spdata.loc[(self.spdata['sm'] <= y*100+m) & (self.spdata['em'] >= y*100+m)]
		return(sp_tomerge.astype(object))




