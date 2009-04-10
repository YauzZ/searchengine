## coding:utf-8 ##
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
				print "indexed :",url
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


class searcher:
	def __init__(self,dbname):
		self.con=sqlite.connect(dbname)

	def __def__(self,dbname):
		self.con.close()
	
	def getmatchrows(self,q):
		fieldlist='w0.urlid'
		tablelist=''
		clauselist=''
		wordids=[]

		words=q.split(' ')
		tablenumber=0

		for word in words:
			wordrow=self.con.execute(
					"select rowid from wordlist where word='%s'" % word).fetchone()
			if wordrow!=None:
				wordid=wordrow[0]
				wordids.append(wordid)
				if tablenumber>0:
					tablelist+=','
					clauselist+=' and '
					clauselist+='w%d.urlid=w%d.urlid and ' % (tablenumber-1,tablenumber)
				fieldlist+=',w%d.location' % tablenumber
				tablelist+='wordlocation w%d' % tablenumber
				clauselist+='w%d.wordid=%d' % (tablenumber,wordid)
				tablenumber+=1
		fullquery='select %s from %s where %s' % (fieldlist,tablelist,clauselist)
		cur=self.con.execute(fullquery)
		rows=[row for row in cur]

		return rows,wordids
	
	def getscoredlist(self,rows,wordids):
		totalscores=dict([(row[0],0) for row in rows])

		weights=[#(1.0,self.frequencyscore(rows)),	#单词频度
				 #(1.0,self.locationscore(rows)),	#文档位置
				 #(1.0,self.distancescore(rows)),	#单词距离
				 (1.0,self.inboundlinkscore(rows))]	#外部回指链接简单计数

		for (weight,scores) in weights:
			for url in totalscores:
				totalscores[url]+=weight*scores[url]

		return totalscores

	def geturlname(self,id):
		return self.con.execute(
				"select url from urllist where rowid=%d" % id).fetchone()[0]

	def query(self,q):
		rows,wordids=self.getmatchrows(q)
		scores=self.getscoredlist(rows,wordids)
		rankedscores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)
		for (score,urlid) in rankedscores[0:10]:
			print '%f\t%s' % (score,self.geturlname(urlid))

	def normalizescores(self,scores,smallIsBetter=0):
		vsmall=0.00001
		if smallIsBetter:
			minscore=min(scores.values())
			return dict([(u,float(minscore)/max(vsmall,l)) for (u,l) \
					in scores.items()])
		else:
			maxscore=max(scores.values())
			if maxscore==0: maxscore=vsmall
			return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])

	def frequencyscore(self,rows):
		counts=dict([(row[0],0) for row in rows])
		for row in rows:
			counts[row[0]]+=1
		return self.normalizescores(counts)

	def locationscore(self,rows):
		locations=dict([(row[0],1000000) for row in rows])
		for row in rows:
			loc=sum(row[1:])
			if loc<locations[row[0]]: 
				locations[row[0]]=loc
		return self.normalizescores(locations,smallIsBetter=1)
	
	def distancescore(self,rows):
		if len(rows[0])<=2:
			return dict([(row[0],1.0) for row in rows])

		mindistance=dict([(row[0],1000000) for row in rows])
		
		for row in rows:
			dist=sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
			if dist<mindistance[row[0]]:
				mindistance[row[0]]=dist
		return self.normalizescores(mindistance,smallIsBetter=1)

	def inboundlinkscore(self,rows):
		uniqueurls=set([row[0] for row in rows])
		inboundcount=dict([(u,self.con.execute( \
						'select count(*) from link where toid=%d' %u).fetchone()[0]) \
						for u in uniqueurls])
		return self.normalizescores(inboundcount)
