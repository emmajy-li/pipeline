from wrdsdata import wrdsdata

class crsp(wrdsdata):
	"""
	
	"""
	def __init__(self, startyear, endyear, month, file):
		"""
		The constructor for crsp class.

		Args: 
           startyear (int): The startyear of the desired output data.
           endyear (int): The endyear of the desired output data.
           month (int): The month of the desired output data; 0 means every month.
           file (str): the file name of the desired output data.

        Returns:
        	no returns.
		"""
		wrdsdata.__init__(self, startyear, endyear, month, file)
		pass