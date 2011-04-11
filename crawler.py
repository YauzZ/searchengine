import searchengine
import threading

pagelist=[ 'http://baike.baidu.com/',
		'http://zhidao.baidu.com/',
	]


crawler=searchengine.crawler('pages')
crawler.createindextables()
crawler.crawl(pagelist,2)
#crawler.calculatepagerank()

