# entrySet()

When the server outbound buffer get full entry can come back in separate messages, the worst case of this is for each entry to come back in a separate message as illistrated below. This is realistically only likely to happend when the size of each entry is very large.
```
final ChronicleMap<Integer, String> map = createMap();

Map<Integer, String> m = new HashMap<>();

for (int i = 0; i < 5; i++) {
    m.put(i, "hello " + i);
}

map.putAll(m);
map.entrySet();
```

Only the entry set is shown below :

--------------------------------------------
client writes:
```
type: MAP
tid: 1426529566218
timeStamp: 1426529566218
channelId: 2
methodName: entrySet
```
--------------------------------------------
server reads:
```
type: MAP
tid: 1426529566218
timeStamp: 1426529566218
channelId: 2
methodName: entrySet
```
--------------------------------------------
server writes:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
```

--------------------------------------------
server writes:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 3"
```

--------------------------------------------
server writes:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 0"
```
--------------------------------------------
server writes:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 1"
```
--------------------------------------------
server writes:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 2"
```
--------------------------------
client read:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
```

--------------------------------
client read:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
```

--------------------------------
client read:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
```

--------------------------------
client read:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
```

--------------------------------
client read:
```
tid: 1426529566218
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
```

--------------------------------
client read:
```
tid: 1426529566218
isException: false
hasNext: false
```

[0=hello 0, 1=hello 1, 2=hello 2, 3=hello 3, 4=hello 4]
