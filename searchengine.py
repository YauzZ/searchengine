## coding:utf-8 ##
import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite
from mmseg import seg_txt
import MySQLdb
import socket

socket.setdefaulttimeout(3)

ignorewords=set(['the','of','to','and','a','in','is','it'])

class crawler:
	def __init__(self,dbname):
		#		self.con=sqlite.connect(dbname)
		self.con = MySQLdb.connect(passwd='jljuji9',db=dbname,user='root')
		pass

	def __del__(self):
		self.con.close()
		pass
	def dbcommit(self):
		self.con.commit()
		pass

	def getentryid(self,table,field,value,createnew=True):
		cur = self.con.cursor()
		cur.execute(
				"select rowid from %s where %s='%s'" % (table,field,value))
		res=cur.fetchone()
		if res==None:
			cur.execute(
					"insert into %s (%s) values ('%s')" %(table,field,value))
			relt=cur.lastrowid
			cur.close()
			return relt
		else:
			return res[0]

	def addtoindex(self,url,soup):
		print 'Indexing %s' % url

		text=self.gettextonly(soup)
		words=self.separatewords(text)

		urlid=self.getentryid('urllist','url',url)

		for i in range(len(words)):
			word=words[i]
			if word in ignorewords:
				continue
			wordid=self.getentryid('wordlist','word',word)
			cur = self.con.cursor()
			cur.execute("insert into wordlocation(urlid,wordid,location) \
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
		#		for s in seg_txt(text.encode('utf-8')):
		#	if s!='' and len(s)>3 and not s.isalnum():
		#		print s
		return [s.lower() for s in seg_txt(text.encode('utf-8')) if s!='' and len(s)>3 and not s.isalnum()]

	def isindexed(self,url):
		cur = self.con.cursor()
		cur.execute("select rowid from urllist where url='%s'" % url)
		u=cur.fetchone()
		if u!=None:
			cur.execute('select * from wordlocation where urlid=%d' % u[0])
			v=cur.fetchone()
			if v!=None:
				print "indexed :",url
				cur.close()
				return True
		cur.close()
		return False

	def addlinkref(self,urlFrom,urlTo,linkText):
		fromid=self.getentryid('urllist','url',urlFrom)
		toid=self.getentryid('urllist','url',urlTo)

		cur = self.con.cursor()
		cur.execute(
				"select rowid from link where fromid='%s' and toid='%s'" % (fromid,toid))
		res=cur.fetchone()
		if res==None:
			cur.execute(
				"insert into link (fromid,toid) values ('%s','%s')" %(fromid,toid))
			linkid=cur.lastrowid
		else:
		    linkid=res[0]

		words=self.separatewords(linkText)
		for word in words:
			wordid=self.getentryid('wordlist','word',word)
			cur.execute("insert into linkwords (wordid,linkid) values ('%s','%s')" %(linkid,wordid))

	def crawl(self,pages,depth=2):
		for i in range(depth):
			newpages=set()
			for page in pages:
				try:
					c=urllib2.urlopen(page)
				except:
					print "Could not open %s" % page
					continue
				try:
					soup=BeautifulSoup(c.read())
				except:
					continue
				if not self.isindexed(page):
					self.addtoindex(page,soup)
				else:
					if depth < 3:
						continue

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
		cur = self.con.cursor()
		cur.execute('create table IF NOT EXISTS urllist(rowid MEDIUMINT NOT NULL AUTO_INCREMENT , \
				url VARCHAR(200), PRIMARY KEY (rowid), index urlidx (url(200)))')
		cur.execute('create table IF NOT EXISTS wordlist(rowid MEDIUMINT NOT NULL AUTO_INCREMENT , \
				word VARCHAR(50), PRIMARY KEY (rowid), index wordidx (word(50)))')
		cur.execute('create table IF NOT EXISTS wordlocation(urlid INT,wordid INT,location VARCHAR(50), \
				index wordurlidx (wordid)) ')
		cur.execute('create table IF NOT EXISTS link(fromid INT,toid INT, \
				rowid MEDIUMINT NOT NULL AUTO_INCREMENT ,PRIMARY KEY (rowid),index urltoidx (toid),\
				index urlfromidx (fromid)) ')
		cur.execute('create table IF NOT EXISTS linkwords(wordid INT,linkid INT) ')
#		cur.execute('create index IF NOT EXISTS wordidx on wordlist(word)')
#		cur.execute('create index IF NOT EXISTS urlidx on urllist(url VARCHAR(50))')
#		cur.execute('create index IF NOT EXISTS wordurlidx on wordlocation(wordid)')
#		cur.execute('create index IF NOT EXISTS urltoidx on link(toid)')
#		cur.execute('create index IF NOT EXISTS urlfrom on link(fromid)')
		cur.close()
		self.dbcommit()

	def calculatepagerank(self,iterations=20):
		cur = self.con.cursor()
		cur.execute('drop table if exists pagerank')
		cur.execute('create table pagerank(urlid INT,score INT, PRIMARY KEY (urlid)) ENGINE=MEMORY')
		cur.execute('insert into pagerank select rowid, 1.0 from urllist')
		self.dbcommit()

		for i in range(iterations):
			print "Iteration %d" % (i)
			cur = self.con.cursor()
			cur.execute('select rowid from urllist')
			for (urlid,) in cur.fetchall():
				pr=0.15
				cur.execute('select distinct fromid from link where toid=%d' % urlid)
				for (linker,) in cur.fetchall():
					cur.execute('select score from pagerank where urlid=%d' % linker)
					linkingpr=cur.fetchone()[0]
					cur.execute('select count(*) from link where fromid=%d' % linker)
					linkingcount=cur.fetchone()[0]
					pr+=0.85*(linkingpr/linkingcount)
					cur.execute('update pagerank set score=%f where urlid=%d' % (pr,urlid))
		self.dbcommit()

if __name__ == "__main__":
	print "searchengine ok"


class searcher:
	def __init__(self,dbname):

		self.con = MySQLdb.connect(passwd='jljuji9',db=dbname,user='root')
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
			cur = self.con.cursor()
			cur.execute(
					"select rowid from wordlist where word='%s'" % word)
			wordrow=cur.fetchone()
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
		cur.execute(fullquery)
		relt=cur.fetchall()
		rows=[row for row in relt]

		return rows,wordids

	def getscoredlist(self,rows,wordids):
		totalscores=dict([(row[0],0) for row in rows])

		weights=[(1.0,self.frequencyscore(rows)),	#单词频度
				# (1.0,self.locationscore(rows)),	#文档位置
				 (1.0,self.distancescore(rows)),	#单词距离
				 (1.0,self.inboundlinkscore(rows)),	#外部回指链接简单计数
				# (1.0,self.pagerankscore(rows)),	#PageRank算法
				 (1.0,self.linktextscore(rows,wordids))	#基于链接文本的PageRank算法
				]

		for (weight,scores) in weights:
			for url in totalscores:
				totalscores[url]+=weight*scores[url]

		return totalscores

	def geturlname(self,id):
		cur = self.con.cursor()
		cur.execute("select url from urllist where rowid=%d" % id)
		return cur.fetchone()[0]

	def query(self,q):
		#try:
			rows,wordids=self.getmatchrows(q)
			scores=self.getscoredlist(rows,wordids)
			rankedscores=sorted([(score,url) for (url,score) in scores.items()],reverse=1)
			for (score,urlid) in rankedscores[0:10]:
				print '%f\t%s' % (score,self.geturlname(urlid))
		#except:
		#	print "出错了"
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
		cur = self.con.cursor()
		uniqueurls=set([row[0] for row in rows])
		inboundcount={}
		for u in uniqueurls:
			cur.execute('select count(*) from link where toid=%d' % u)
			inboundcount[u]=cur.fetchone()[0]
		return self.normalizescores(inboundcount)

	def pagerankscore(self,rows):
		cur = self.con.cursor()
		cur.execute('select score from pagerank whereurlid=%d' % row[0])
		pageranks=dict([(row[0],cur.fetchone()[0]) for row in rows])

		#maxrank=max(pageranks.values())
		#normalizedscores=dict([(u,float(1)/maxrank) for (u,l) in pageranks.items()])
		#return normalizedscores
		return self.normalizescores(pageranks)

	def linktextscore(self,rows,wordids):
		cur = self.con.cursor()
		linkscores=dict([(row[0],0) for row in rows])
		for wordid in wordids:
			cur.execute('select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid' % wordid)
			relt=cur.fetchall()
			for (fromid,toid) in relt:
				if fromid in linkscores:
					cur.execute('select score from pagerank where urlid=%d' % fromid)
					pr=cur.fetchone()[0]
					linkscores[fromid]+=pr

		#maxscore=max(linkscores.values())
		#normalizedscores=dict([(u,float(l)/maxscore) for (u,l) in linkscores.items()])
		#return normalizedscores
		return self.normalizescores(linkscores)

