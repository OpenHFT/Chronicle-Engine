# toString()

when the server outbound buffer get full each entry in the entryset can be sent separately like
this :

```
final ChronicleMap<Integer, String> map = createMap();

Map<Integer, String> m = new HashMap<>();

for (int i = 0; i < 5; i++) {
    m.put(i, "hello " + i);
}

map.putAll(m);
map.entrySet();
```

client writes:
```

type: MAP
transactionId: 1426529566212
timeStamp: 1426529566212
channelId: 2
methodName: putAll
hasNext: true
arg1: 0
arg2: "hello 0"
hasNext: true
arg1: 1
arg2: "hello 1"
hasNext: true
arg1: 2
arg2: "hello 2"
hasNext: true
arg1: 3
arg2: "hello 3"
hasNext: false
arg1: 4
arg2: "hello 4"
```
--------------------------------------------
server reads:
```

type: MAP
transactionId: 1426529566212
timeStamp: 1426529566212
channelId: 2
methodName: putAll
hasNext: true
arg1: 0
arg2: "hello 0"
hasNext: true
arg1: 1
arg2: "hello 1"
hasNext: true
arg1: 2
arg2: "hello 2"
hasNext: true
arg1: 3
arg2: "hello 3"
hasNext: false
arg1: 4
arg2: "hello 4"
```
--------------------------------------------
server writes:
```
transactionId: 1426529566212
isException: false
```
--------------------------------
client read:
```
transactionId: 1426529566212
isException: false
```

--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426529566218
timeStamp: 1426529566218
channelId: 2
methodName: entrySet
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426529566218
timeStamp: 1426529566218
channelId: 2
methodName: entrySet
```
--------------------------------------------
server writes:
```
transactionId: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
```

--------------------------------
client read:
```
transactionId: 1426529566218
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
```

--------------------------------
client read:
```
transactionId: 1426529566218
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
```

--------------------------------
client read:
```
transactionId: 1426529566218
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
```

--------------------------------
client read:
```
transactionId: 1426529566218
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
```

--------------------------------
client read:
```
transactionId: 1426529566218
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
```

--------------------------------
client read:
```
transactionId: 1426529566218
isException: false
hasNext: false


--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426529566266
timeStamp: 1426529566266
channelId: 2
methodName: entrySet
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426529566266
timeStamp: 1426529566266
channelId: 2
methodName: entrySet
```
--------------------------------------------
server writes:
```
transactionId: 1426529566266
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
```

--------------------------------
client read:
```
transactionId: 1426529566266
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
```

--------------------------------
client read:
```
transactionId: 1426529566266
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
```

--------------------------------
client read:
```
transactionId: 1426529566266
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
```

--------------------------------
client read:
```
transactionId: 1426529566266
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
```

--------------------------------
client read:
```
transactionId: 1426529566266
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
```

--------------------------------
client read:
```
transactionId: 1426529566266
isException: false
hasNext: false
```

[0=hello 0, 1=hello 1, 2=hello 2, 3=hello 3, 4=hello 4]