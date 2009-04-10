import searchengine
pagelist=['http://kiwitobes.com/wiki/Perl.html','http://kiwitobes.com/wiki/Categorical_list_of_programming_languages.html']
crawler=searchengine.crawler('searchindex.db')
crawler.createindextables()
crawler.crawl(pagelist)
