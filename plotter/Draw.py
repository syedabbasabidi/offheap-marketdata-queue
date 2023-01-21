import matplotlib.pyplot as plt


f, test = plt.subplots()
xdata = [0,1,2,5,6,9,10]
condata = [0.02, 0.03, 0.06, 1.13, 1.84, 2.05, 2.51]
conCOData = [0.02, 0.03, 4907.01, 9093.13, 10272.84, 10862.05, 11059.51]

prodCOdata = [0.02, 0.02, 10.67, 32.96, 2813.95, 4042.05, 4399.51]
proddata = [0.02, 0.02, 1.33, 1.57, 1.76, 26.98, 28.58]


prettyxdata = xdata
for i in range(len(xdata)):
    if prettyxdata [i] == 5:
        prettyxdata [i]=2
    elif prettyxdata [i] == 6:
        prettyxdata [i]=3
    elif prettyxdata [i] == 9:
        prettyxdata [i]=4
    elif prettyxdata [i] == 10:
        prettyxdata [i]=5

f, test = plt.subplots()
test.plot(prettyxdata,condata,'o--', alpha=1, label='Con')
test.plot(prettyxdata,proddata,'o--', alpha=1, label='Prod')

test.set_xticks([0,1,2,3,4,5,6],['50', '90', '99', '99.7', '99.9', '99.97', '99.99'])
plt.ylabel('Time - micorsecond')
plt.xlabel('Percentile')

plt.legend()
plt.title('LinearQueue - 10M MarketData (45B) Ticks (1M/sec)')
plt.grid()
plt.show()