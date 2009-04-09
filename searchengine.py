


class crawler:
	def __init__(self,dbname):
		pass

	def __del__(self):
		pass
	def dbcommit(self):
		pass
	
	def geientryid(self,table,field,value,createnew=True):
		return None
	
	def addtoindex(self,url,soup):
		print 'Indexing %s' % url

	def gettextonly(self,soup):
		return None

	def separatewords(self,text):
		return None

	def isindexed(self,url):
		return False

	def addlinkref(self,urlFrom,urlTo,linkText):
		pass

	def crawl(self,pages,depth=2):
		pass

	def createindextables(self):
		pass
