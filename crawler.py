import searchengine
import threading

pagelist=[	'http://kiwitobes.com/wiki/Python_programming_language.html',
<<<<<<< HEAD:crawler.py
		'http://kiwitobes.com/wiki/Perl.html',
		'http://kiwitobes.com/wiki/Java_programming_language.html'
	]
=======
			#'http://kiwitobes.com/wiki/Perl.html',
			#'http://kiwitobes.com/wiki/Java_programming_language.html'
			]
>>>>>>> f3bb515... 改进查询的界面:crawler.py





crawler=searchengine.crawler('searchindex.db')
#crawler.calculatepagerank()
crawler.createindextables()
crawler.crawl(pagelist,2)
crawler.calculatepagerank()

