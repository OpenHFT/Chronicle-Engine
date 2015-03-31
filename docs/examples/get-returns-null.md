# get returns a NULL

```
  map.get(1);
```

--------------------------------------------

client writes:

```
type: MAP
tid: 1426515026166
timeStamp: 1426515026166
channelId: 2
methodName: get
arg1: 1
```

--------------------------------------------

server reads:

```
type: MAP
tid: 1426515026166
timeStamp: 1426515026166
channelId: 2
methodName: get
arg1: 1
```
--------------------------------------------

server writes:

```
tid: 1426515026166
isException: false
resultIsNull: true
```

--------------------------------

client read:
```
tid: 1426515026166
isException: false
resultIsNull: true
```