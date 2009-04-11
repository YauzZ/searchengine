import searchengine
pagelist=['http://kiwitobes.com/wiki/Python_programming_language.html',
			'http://kiwitobes.com/wiki/Perl.html',
			'http://kiwitobes.com/wiki/Java_programming_language.html']
crawler=searchengine.crawler('searchindex.db')
crawler.createindextables()
crawler.crawl(pagelist,3)
