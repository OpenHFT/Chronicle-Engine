# get(<key>)

```
 map.get(1);
```

client writes:
```
type: MAP
tid: 1426514874407
timeStamp: 1426514874407
channelId: 2
methodName: get
arg1: 1
```
--------------------------------------------
server reads:
```
type: MAP
tid: 1426514874407
timeStamp: 1426514874407
channelId: 2
methodName: get
arg1: 1
```
--------------------------------------------
server writes:
```
tid: 1426514874407
isException: false
resultIsNull: false
result: hello

```
--------------------------------
client read:
```
tid: 1426514874407
isException: false
resultIsNull: false
result: hello
```