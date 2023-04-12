import matplotlib.pyplot as plt


f, test = plt.subplots()
xdata = [0,1,2,5,6,9,10]
condata = [0.46, 0.47, 0.49, 0.49, 0.51, 1.44, 2.32]
conCOData = [0.02, 0.03, 4907.01, 9093.13, 10272.84, 10862.05, 11059.51]

prodCOdata = [0.02, 0.02, 10.67, 32.96, 2813.95, 4042.05, 4399.51]
proddata = [0.10, 0.11, 0.12, 0.16, 0.17, 1.07, 1.75]


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
plt.title('CircularQueue - 5M MarketData Ticks (1M/sec)')
plt.grid()
plt.show()