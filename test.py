import searchengine
pagelist=['http://kiwitobes.com/wiki/Perl.html']
crawler=searchengine.crawler('searchindex.db')
crawler.createindextables()
crawler.crawl(pagelist)
