import searchengine

pagelist=[ 'http://baike.baidu.com/',
		'http://zhidao.baidu.com/',
		'http://www.taobao.com/',
	]


crawler=searchengine.crawler('pages')
rawler.createindextables()
crawler.crawl(pagelist,3)

