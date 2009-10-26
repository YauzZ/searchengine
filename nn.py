
from math import tanh
from pysqlite2 import dbapi2 as sqlite


def dtanh(y):
    return 1.0-y*y


class searchnet:
    def __init__(self,dbname):
        self.con=sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def maketables(self):
        self.con.execute('create table IF NOT EXISTS hiddennode(create_key)')
        self.con.execute('create table IF NOT EXISTS wordhidden(fromid,toid,strength)')
        self.con.execute('create table IF NOT EXISTS hiddenurl(fromid,toid,strength)')
        self.con.commit()

    def getstrength(self,fromid,toid,layer):
        if layer==0:
            table='wordhidden'
        else:
            table='hiddenurl'

        res=self.con.execute('select strength from %s where fromid=%d and toid=%d' % (table,fromid,toid)).fetchone()
        if res==None:
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
            self.con.execute('insert into %s (fromid,toid,strength) values (%d,%d,%f)' %(table,fromid,toid,strength))
        else:
            rowid=res[0]
            self.con.execute('update %s set strength=%f where rowid=%d' % (table,strength,rowid))


    def generatehiddennode(self,wordids,urls):
        if len(wordids)>3:
            return None
        createkey='_'.join(sorted([str(wi) for wi in wordids]))
        res=self.con.execute("select rowid from hiddennode where create_key='%s'" % createkey ).fetchone()

        if res==None:
             cur=self.con.execute("insert into hiddennode (create_key) values ('%s')" % createkey)
             hiddenid=cur.lastrowid
             for wordid in wordids:
                 self.setstrength(wordid,hiddenid,0,1.0/len(wordids))
             for urlid in urls:
                 self.setstrength(hiddenid,urlid,1,0.1)
             self.con.commit()

    def getallhiddenids(self,wordids,urlids):
        l1={}
        for wordid in wordids:
            cur=self.con.execute('select toid from wordhidden where fromid=%d' % wordid)
            for row in cur:
                l1[row[0]]=1
        for urlid in urlids:
            cur=self.con.execute('select fromid from hiddenurl where toid=%d' % urlid)
            for row in cur:
                l1[row[0]]=1

        return l1.keys()

    def setupnetwork(self,wordids,urlids):
        self.wordids=wordids
        self.hiddenids=self.getallhiddenids(wordids,urlids)
        self.urlids=urlids

        self.ai = [1.0]*len(self.wordids)
        self.ah = [1.0]*len(self.hiddenids)
        self.ao = [1.0]*len(self.urlids)

        self.wi = [[self.getstrength(wordid,hiddenid,0)
                    for hiddenid in self.hiddenids]
                    for wordid in self.wordids]

        self.wo = [[self.getstrength(hiddenid,urlid,1)
                    for urlid in self.urlids]
                    for hiddenid in self.hiddenids]

    def feedforward(self):
        for i in range(len(self.wordids)):
            self.ai[i] = 1.0

        for j in range(len(self.hiddenids)):
            sum = 0.0
            for i in range(len(self.wordids)):
                sum = sum + self.ai[i] * self.wi[i][j]
            self.ah[j] = tanh(sum)

        for k in range(len(self.urlids)):
            sum = 0.0
            for j in range(len(self.hiddenids)):
                sum = sum + self.ah[j] * self.wo[j][k]
            self.ao[k] = tanh(sum)

        return self.ao[:]

    def getresult(self,wordids,urlids):
        self.setupnetwork(wordids,urlids)
        return self.feedforward()

    def backPropagate(self,targets,N=0.5):
        output_deltas = [0.0] * len(self.urlids)
        for k in range(len(self.urlids)):
            error = targets[k]-self.ao[k]
            output_deltas[k] = dtanh(self.ao[k]) * error

        hidden_deltas = [0.0] * len(self.hiddenids)
        for j in range(len(self.hiddenids)):
            error = 0.0
            for k in range(len(self.urlids)):
                error = error + output_deltas[k] * self.wo[j][k]
            hidden_deltas[j] = dtanh(self.ah[j]) * error

        for j in range(len(self.hiddenids)):
            for k in range(len(self.urlids)):
                change = output_deltas[k] * self.ah[j]
                self.wo[j][k] = self.wo[j][k] + N*change

        for i in range(len(self.wordids)):
            for j in range(len(self.hiddenids)):
                change = hidden_deltas[j] * self.ai[i]
                self.wi[i][j] = self.wi[i][j] + N*change

    def trainquery(self,wordids,urlids,selectedurl):
        self.generatehiddennode(wordids,urlids)

        self.setupnetwork(wordids,urlids)
        self.feedforward()
        targets = [0.0]*len(urlids)
        targets[urlids.index(selectedurl)] = 1.0
        self.backPropagate(targets)
        self.updatedatabase()

    def updatedatabase(self):
        for i in range(len(self.wordids)):
            for j in range(len(self.hiddenids)):
                self.setstrength(self.wordids[i],self.hiddenids[j],0,self.wi[i][j])
        for j in range(len(self.hiddenids)):
            for k in range(len(self.urlids)):
                self.setstrength(self.hiddenids[j],self.urlids[k],1,self.wo[j][k])
        self.con.commit()

