import pandas as pd
import numpy as np
import csv

class crsp:
	def __init__(self, startyear, endyear, month=0):
		self.start=startyear
		self.end=endyear
		self.month=month
		self.d={}
		self.initDataFrame()

	def initDataFrame(self):
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					self.d[filename]=pd.DataFrame()
			else:
				filename=self.getfilename(y, self.month)
				self.d[filename]=pd.DataFrame()

	def getfilename(self, y, m):
		if m < 10:
			filename='crsp_{y}0{m}'.format(y=y,m=m)
		else:
			filename='crsp_{y}{m}'.format(y=y,m=m)
		return(filename)

	
	def split(self, data):
		dt = data.applymap(str)
		for y in range(self.start, self.end+1):
			if self.month==0:
				for m in range(1,13):
					filename=self.getfilename(y, m)
					self.d[filename]=self.d[filename].append(dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])])
			else:
				filename=self.getfilename(y, self.month)
				self.d[filename]=self.d[filename].append(dt.loc[dt['date'].str.startswith(filename[len(filename)-6:])])

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

	def print(self, year, month):
		filename = self.getfilename(year, month)
		print("shape: ",self.d[filename].shape)
		print("head: ",self.d[filename].head())

