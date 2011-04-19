import searchengine

pagelist=[
		#'http://www.cnbeta.com'
		'http://www.talkcc.com',
		#'http://obmem.info',
	]

crawler=searchengine.crawler('pages')
crawler.createindextables()
crawler.crawl(pagelist,3)

