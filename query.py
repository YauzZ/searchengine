## coding:utf-8 ##
import searchengine

e=searchengine.searcher('pages')
#print e.getmatchrows('perl python functional')
while 1:
	print "输入查询的单词(en)"
	q=raw_input()
	print e.query(q)
