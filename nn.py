
from math into tanh
from pysqlite import dbapi2 as sqlite

class searchnet:
    def __init__(self,dbname):
        self.con=sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def maketable(self):
        self.execute('create table hiddennode(create_key)')
        self.execute('create table wordhidden(fromid,toid,strength)')
        self.execute('create table hiddenurl(fromid,toid,strength)')
        self.con.commit()

    def getstrength(self,fromid,toid,layer):
        if layer=0:
            table='wordhidden'
        else:
            table='hiddenurl'

        res=self.con.execute('select strength from %s where fromid=%d and toid=%d' % (table,fromid,toid)).fetchone()
        if res=None:
            if layer==0:
                return -0.2
            if layer==1:
                return 0

        return res[0]

    def setstrength(self,fromid,toid,layer,strength):
        if layer==0:
            table='wordhidden'
        else:
            table='hiddenurl'

        res=self.con.execute('select rowid from %s where fromid=%d and toid=%d' % (table,fromid,toid)).fetchone()

        if res==None:
            self.ececute('insert into %s (fromid,toid,strength) values (%d,%d,%f)' %(table,fromid,toid,strength))
        else:
            rowid=res[0]
            self.con.execute('update %s set strength=%f where rowid=%d' % (table,strength,rowid))



