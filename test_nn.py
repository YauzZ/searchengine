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
