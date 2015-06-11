# get(<key>) returning NULL

```
 ChronicleMap<Integer, String> map = createMap();
 map.put(1, "hello");
```

client writes:

```
type: MAP
tid: 1426514770289
timeStamp: 1426514770289
channelId: 2
methodName: put
arg1: 1
arg2: hello
```
--------------------------------------------
server reads:

```
type: MAP
tid: 1426514770289
timeStamp: 1426514770289
channelId: 2
methodName: put
arg1: 1
arg2: hello
```
--------------------------------------------
server writes:
```
tid: 1426514770289
isException: false
resultIsNull: true

```
--------------------------------
client read:
```
tid: 1426514770289
isException: false
resultIsNull: true
```