import pandas as pd
import numpy as np
import csv
import dataset

class master:
	def __init__(self, startyear, endyear, month=0, file):
		dataset.__init__(self, startyear, endyear, month)
		self.file = file

	