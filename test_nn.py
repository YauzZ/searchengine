import nn

mynet=nn.searchnet('nn.db')
mynet.maketables()
wWorld,wRiver,wBank =101,102,103
uWorldBank,uRiver,uEarth =201,202,203

mynet.generatehiddennode([wWorld,wBank],[uWorldBank,uRiver,uEarth])

print "wordhidden:"
for c in mynet.con.execute('select * from wordhidden'):
    print c

print "hiddenurl:"
for c in mynet.con.execute('select * from hiddenurl'):
    print c

print mynet.getresult([wWorld,wBank],[uWorldBank,uRiver,uEarth])

allurls=[uWorldBank,uRiver,uEarth]

for i in range(30):
    mynet.trainquery([wWorld,wBank],allurls,uWorldBank)
    mynet.trainquery([wRiver,wBank],allurls,uRiver)
    mynet.trainquery([wWorld],allurls,uEarth)


print mynet.getresult([wWorld,wBank],allurls)

print mynet.getresult([wRiver,wBank],allurls)

print mynet.getresult([wBank],allurls)


