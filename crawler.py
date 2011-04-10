import searchengine
import threading

pagelist=[ 'http://www.taobao.com/',
	]


crawler=searchengine.crawler('pages')
crawler.createindextables()
crawler.crawl(pagelist,2)
crawler.calculatepagerank()

