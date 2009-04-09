import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite

ignorewords=set(['the','of','to','and','a','in','is','it'])

class crawler:
	def __init__(self,dbname):
		self.con=sqlite.connect(dbname)
		pass

	def __del__(self):
		self.con.close()
		pass
	def dbcommit(self):
		self.con.commit()
		pass
	
	def getentryid(self,table,field,value,createnew=True):
		cur=self.con.execute(
				"select rowid from %s where %s='%s'" % (table,field,value))
		res=cur.fetchone()
		if res==None:
			cur=self.con.execute( 
					"insert into %s (%s) values ('%s')" %(table,field,value))
			return cur.lastrowid
		else:
			return res[0]
	
	def addtoindex(self,url,soup):
		if self.isindexed(url):
			return True
		print 'Indexing %s' % url 

		text=self.gettextonly(soup)
		words=self.separatewords(text)
		
		urlid=self.getentryid('urllist','url',url)

		for i in range(len(words)):
			word=words[i]
			if word in ignorewords:
				continue
			wordid=self.getentryid('wordlist','word',word)
			self.con.execute("insert into wordlocation(urlid,wordid,location) \
					values (%d,%d,%d)" % (urlid,wordid,i))

	

	def gettextonly(self,soup):
		v=soup.string
		if v==None:
			c=soup.contents
			resulttext=''
			for t in c:
				subtext=self.gettextonly(t)
				resulttext+=subtext+'\n'
			return resulttext
		else:
			return v.strip()

	def separatewords(self,text):
		splitter=re.compile('\\W*')
		return [s.lower() for s in splitter.split(text) if s!='']

	def isindexed(self,url):
		u=self.con.execute \
		  ("select rowid from urllist where url='%s'" % url).fetchone()
		if u!=None:
			v=self.con.execute(
					'select * from wordlocation where urlid=%d' % u[0]).fetchone()
			if v!=None:
				return True
				
		return False

	def addlinkref(self,urlFrom,urlTo,linkText):
		pass

	def crawl(self,pages,depth=2):
		for i in range(depth):
			newpages=set()
			for page in pages:
				try:
					c=urllib2.urlopen(page)
				except:
					print "Could not open %s" % page
					continue
				soup=BeautifulSoup(c.read())
				self.addtoindex(page,soup)

				links=soup('a')
				for link in links:
					if('href' in dict(link.attrs)):
						url=urljoin(page,link['href'])
						if url.find("'")!=-1: continue
						url=url.split('#')[0]
						if url[0:4]=='http' and not self.isindexed(url):
							newpages.add(url)
						linkText=self.gettextonly(link)
						self.addlinkref(page,url,linkText)

				self.dbcommit()
			pages=newpages

	def createindextables(self):
		self.con.execute('create table IF NOT EXISTS urllist(url)')
		self.con.execute('create table IF NOT EXISTS wordlist(word)')
		self.con.execute('create table IF NOT EXISTS wordlocation(urlid,wordid,location)')
		self.con.execute('create table IF NOT EXISTS link(fromid integer,toid integer)')
		self.con.execute('create table IF NOT EXISTS linkwords(wordid,linkid)')
		self.con.execute('create index IF NOT EXISTS wordidx on wordlist(word)')
		self.con.execute('create index IF NOT EXISTS urlidx on urllist(url)')
		self.con.execute('create index IF NOT EXISTS wordurlidx on wordlocation(wordid)')
		self.con.execute('create index IF NOT EXISTS urltoidx on link(toid)')
		self.con.execute('create index IF NOT EXISTS urlfrom on link(fromid)')
		self.dbcommit()
		pass

if __name__ == "__main__":
	print "searchengine ok"
