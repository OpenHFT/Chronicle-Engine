# entrySet()

Its possible for the server to provide the entrySet() in a single message.

```
final ChronicleMap<Integer, String> map = createMap();

Map<Integer, String> m = new HashMap<>();

for (int i = 0; i < 5; i++) {
    m.put(i, "hello " + i);
}

map.putAll(m);
map.entrySet();
```


--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426529866215
timeStamp: 1426529866215
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
transactionId: 1426529866215
timeStamp: 1426529866215
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
transactionId: 1426529866215
isException: false
```

--------------------------------
client read:
```
transactionId: 1426529866215
isException: false
```

--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426529866222
timeStamp: 1426529866222
channelId: 2
methodName: entrySet
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426529866222
timeStamp: 1426529866222
channelId: 2
methodName: entrySet
```
--------------------------------------------
server writes:

transactionId: 1426529866222
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
isException: false
hasNext: false
```

--------------------------------
client read:
```
transactionId: 1426529866222
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
isException: false
hasNext: false
```

--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426529866270
timeStamp: 1426529866270
channelId: 2
methodName: entrySet
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426529866270
timeStamp: 1426529866270
channelId: 2
methodName: entrySet
```
--------------------------------------------
server writes:
```
transactionId: 1426529866270
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
isException: false
hasNext: false
```

--------------------------------
client read:
```
transactionId: 1426529866270
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
isException: false
hasNext: true
resultKey: 0
resultValue: "hello 0"
isException: false
hasNext: true
resultKey: 1
resultValue: "hello 1"
isException: false
hasNext: true
resultKey: 2
resultValue: "hello 2"
isException: false
hasNext: false
```
[0=hello 0, 1=hello 1, 2=hello 2, 3=hello 3, 4=hello 4]