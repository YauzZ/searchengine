import searchengine
import threading
from DBUtils.PooledDB import PooledDB
from pysqlite2 import dbapi2 as sqlite

pagelist=[	'http://kiwitobes.com/wiki/Python_programming_language.html',
		'http://kiwitobes.com/wiki/Perl.html',
		'http://kiwitobes.com/wiki/Java_programming_language.html'
         ]


pool = PooledDB(sqlite, 10, database='searchindex.db')


def do_crawl():
	crawler=searchengine.crawler(pool)
	crawler.createindextables()
	#crawler.crawl(pagelist,2)
	#crawler.calculatepagerank()

for url in pagelist:
        t = threading.Thread(target=do_crawl)
	t.start()
