import dataset

class master(dataset):
	def __init__(self, startyear, endyear, month=0, file):
		dataset.__init__(self, startyear, endyear, month)
		self.file = file

	
