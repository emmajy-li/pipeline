from dataset import dataset

class crsp(dataset):
	def __init__(self, startyear, endyear, month, file):
		dataset.__init__(self, startyear, endyear, month, file)