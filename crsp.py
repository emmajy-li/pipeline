from dataset import dataset

class crsp(dataset):
	def __init__(self, startyear, endyear, month, file):
		dataset.__init__(self, startyear, endyear, month, file)

	def checkdup(self, y, m):
		filename=self.getfilename(y, m)
		if self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].empty:
			print("No Duplication")
		else: # to be tested
			print('Duplicates', self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].shape)
			print('Pre-Data', self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].shape)
			self.d[filename] = self.d[filename].groupby(list(self.d[filename].columns)[:-1], as_index=False)['sp'].sum()
			print('Post-Data', self.d[filename][self.d[filename].duplicated(list(self.d[filename].columns)[:-1])].shape)
			if self.d[filename]['sp'] > 1:
				raise ValueError('sp indicator should not be less than 1!')