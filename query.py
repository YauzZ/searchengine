import searchengine

e=searchengine.searcher('searchindex.db')
#print e.getmatchrows('perl python functional')
print e.query('perl python')
