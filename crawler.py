import searchengine

pagelist=[ 'http://www.talkcc.com',
	]


crawler=searchengine.crawler('pages')
crawler.createindextables()
crawler.crawl(pagelist,2)

