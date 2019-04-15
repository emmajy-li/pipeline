class dataset:

	def __init__(self, startyear, endyear, month=0):
		self.start=startyear
		self.end=endyear
		self.month=month
		self.d={}
		self.initDataFrame()

	def getfilename(self, y, m):
		if m < 10:
			filename='_'.join([self.file,'{y}0{m}']).format(y=y,m=m)
		else:
			filename='_'.join([self.file,'{y}0{m}']).format(y=y,m=m)
		return(filename)

	def select(self, y, m):
		filename = self.getfilename(y, m)
		return(self.d[filename])

	def initDataFrame(self):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					self.select(y, m) = pd.DataFrame()
			else:
				self.select(y, self.month) = pd.DataFrame()

	def split(self, data):
		dt = data.applymap(str)
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					self.d[filename]=self.d[filename].append(dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])])
			else:
				filename = self.getfilename(y, self.month)
				self.d[filename]=self.d[filename].append(dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])])

	def allmerge(self, data, how='left', key=None, left_on=None, right_on=None):
		dt = data.applymap(str)
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					self.d[filename]=self.d[filename].merge(right=dt, how=how, on=key, left_on=left_on, right_on=right_on)
			else:
				filename = self.getfilename(y, self.month)
				self.d[filename]=self.d[filename].merge(right=dt, how=how, on=key, left_on=left_on, right_on=right_on)

	def onemerge(self, data, how='left', key=None, left_on=None, right_on=None, y, m):
		dt = data.applymap(str)
		filename = self.getfilename(y, m)
		self.d[filename]=self.d[filename].merge(right=dt, how=how, on=key, left_on=left_on, right_on=right_on)

	def addsp(self):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					cond = (self.d[filename]['date']>=self.d[filename]['start']) & (self.d[filename]['date']<=self.d[filename]['ending'])
					self.d[filename]['sp'] = np.where(cond, 1, 0)
			else:
				filename=self.getfilename(y, self.month)
				cond = (self.d[filename]['date']>=self.d[filename]['start']) & (self.d[filename]['date']<=self.d[filename]['ending'])
				self.d[filename]['sp'] = np.where(cond, 1, 0)

	def export(self):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					with open(filename+'.csv', 'a') as csvFile:
						self.d[filename].to_csv(csvFile, header=True)
			else:
				filename=self.getfilename(y, self.month)
				with open(filename+'.csv', 'a') as csvFile:
					self.d[filename].to_csv(csvFile, header=True)

	def print(self, y, m):
		print("shape: ",self.select(y, m).shape)
		print("head: ",self.select(y, m).head())

