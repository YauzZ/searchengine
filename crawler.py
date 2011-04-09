import searchengine
import threading

pagelist=[ 'http://baike.baidu.com/',
	]


crawler=searchengine.crawler('searchindex.db')
crawler.createindextables()
crawler.crawl(pagelist,2)
crawler.calculatepagerank()

