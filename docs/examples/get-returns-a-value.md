# get(<key>)

```
 map.get(1);
```

client writes:
```
type: MAP
transactionId: 1426514874407
timeStamp: 1426514874407
channelId: 2
methodName: get
arg1: 1
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426514874407
timeStamp: 1426514874407
channelId: 2
methodName: get
arg1: 1
```
--------------------------------------------
server writes:
```
transactionId: 1426514874407
isException: false
resultIsNull: false
result: hello

```
--------------------------------
client read:
```
transactionId: 1426514874407
isException: false
resultIsNull: false
result: hello
```