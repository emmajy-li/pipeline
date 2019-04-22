from dataset import dataset

class master(dataset):
	def __init__(self, startyear, endyear, month, file):
		dataset.__init__(self, startyear, endyear, month, file)
		pass

	def add8CUSIP(self, y, m, newcolname):
		filename=self.getfilename(y, m)
		self.d[filename][newcolname] = [value[:8] for value in self.d[filename]['CUSIP']]
		pass

	
