# toString

for a map with more than 20 entries, just the first 20 records are returned and
displayed in the
toString()

like this :

```
{96=hello 96, 33=hello 33, 97=hello 97, 35=hello 35, 3=hello 3, 4=hello 4, 103=hello 103,
105=hello 105, 15=hello 15, 16=hello 16, 80=hello 80, 81=hello 81, 49=hello 49, 52=hello
52, 85=hello 85, 22=hello 22, 24=hello 24, 59=hello 59, 60=hello 60, 62=hello 62, ...
```

```
 ChronicleMap<Integer, String> map = createMap();
for (int i = 0; i < 110; i++) {
    map.put(i, "hello " + i);
}

System.out.println(map.toString());
```

Note : only showing the toSting() messages

--------------------------------------------
client writes:
```
type: MAP
tid: 1426520061803
timeStamp: 1426520061803
channelId: 2
methodName: entrySetRestricted
arg1: 20
```
--------------------------------------------
server reads:
```
type: MAP
tid: 1426520061803
timeStamp: 1426520061803
channelId: 2
methodName: entrySetRestricted
arg1: 20
```
--------------------------------------------
server writes:
```
tid: 1426520061803
isException: false
hasNext: true
resultKey: 33
resultValue: "hello 33"
isException: false
hasNext: true
resultKey: 103
resultValue: "hello 103"
isException: false
hasNext: true
resultKey: 105
resultValue: "hello 105"
isException: false
hasNext: true
resultKey: 24
resultValue: "hello 24"
isException: false
hasNext: true
resultKey: 97
resultValue: "hello 97"
isException: false
hasNext: true
resultKey: 60
resultValue: "hello 60"
isException: false
hasNext: true
resultKey: 16
resultValue: "hello 16"
isException: false
hasNext: true
resultKey: 59
resultValue: "hello 59"
isException: false
hasNext: true
resultKey: 52
resultValue: "hello 52"
isException: false
hasNext: true
resultKey: 35
resultValue: "hello 35"
isException: false
hasNext: true
resultKey: 62
resultValue: "hello 62"
isException: false
hasNext: true
resultKey: 81
resultValue: "hello 81"
isException: false
hasNext: true
resultKey: 49
resultValue: "hello 49"
isException: false
hasNext: true
resultKey: 85
resultValue: "hello 85"
isException: false
hasNext: true
resultKey: 96
resultValue: "hello 96"
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
isException: false
hasNext: true
resultKey: 80
resultValue: "hello 80"
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
isException: false
hasNext: true
resultKey: 15
resultValue: "hello 15"
isException: false
hasNext: true
resultKey: 22
resultValue: "hello 22"
isException: false
hasNext: false

```
--------------------------------
client read:
```
tid: 1426520061803
isException: false
hasNext: true
resultKey: 33
resultValue: "hello 33"
isException: false
hasNext: true
resultKey: 103
resultValue: "hello 103"
isException: false
hasNext: true
resultKey: 105
resultValue: "hello 105"
isException: false
hasNext: true
resultKey: 24
resultValue: "hello 24"
isException: false
hasNext: true
resultKey: 97
resultValue: "hello 97"
isException: false
hasNext: true
resultKey: 60
resultValue: "hello 60"
isException: false
hasNext: true
resultKey: 16
resultValue: "hello 16"
isException: false
hasNext: true
resultKey: 59
resultValue: "hello 59"
isException: false
hasNext: true
resultKey: 52
resultValue: "hello 52"
isException: false
hasNext: true
resultKey: 35
resultValue: "hello 35"
isException: false
hasNext: true
resultKey: 62
resultValue: "hello 62"
isException: false
hasNext: true
resultKey: 81
resultValue: "hello 81"
isException: false
hasNext: true
resultKey: 49
resultValue: "hello 49"
isException: false
hasNext: true
resultKey: 85
resultValue: "hello 85"
isException: false
hasNext: true
resultKey: 96
resultValue: "hello 96"
isException: false
hasNext: true
resultKey: 4
resultValue: "hello 4"
isException: false
hasNext: true
resultKey: 80
resultValue: "hello 80"
isException: false
hasNext: true
resultKey: 3
resultValue: "hello 3"
isException: false
hasNext: true
resultKey: 15
resultValue: "hello 15"
isException: false
hasNext: true
resultKey: 22
resultValue: "hello 22"
isException: false
hasNext: false
```

```
{96=hello 96, 33=hello 33, 97=hello 97, 35=hello 35, 3=hello 3, 4=hello 4, 103=hello 103, 105=hello 105, 15=hello 15, 16=hello 16, 80=hello 80, 81=hello 81, 49=hello 49, 52=hello 52, 85=hello 85, 22=hello 22, 24=hello 24, 59=hello 59, 60=hello 60, 62=hello 62, ...
```
