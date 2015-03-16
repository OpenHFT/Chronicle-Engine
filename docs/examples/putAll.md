# get(key)

``` java
final ChronicleMap<Integer, String> map = createMap();

Map<Integer, String> m = new HashMap<>();

for (int i = 0; i < 110; i++) {
    m.put(i, "hello " + i);
}
map.putAll(m);
```

--------------------------------------------

client writes:

```1
type: MAP
transactionId: 1426526752786
timeStamp: 1426526752786
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
hasNext: true
arg1: 4
arg2: "hello 4"
hasNext: true
arg1: 5
arg2: "hello 5"
hasNext: true
arg1: 6
arg2: "hello 6"
hasNext: true
arg1: 7
arg2: "hello 7"
hasNext: true
arg1: 8
arg2: "hello 8"
hasNext: true
arg1: 9
arg2: "hello 9"
hasNext: true
arg1: 10
arg2: "hello 10"
hasNext: true
arg1: 11
arg2: "hello 11"
hasNext: true
arg1: 12
arg2: "hello 12"
hasNext: true
arg1: 13
arg2: "hello 13"
hasNext: true
arg1: 14
arg2: "hello 14"
hasNext: true
arg1: 15
arg2: "hello 15"
hasNext: true
arg1: 16
arg2: "hello 16"
hasNext: true
arg1: 17
arg2: "hello 17"
hasNext: true
arg1: 18
arg2: "hello 18"
hasNext: true
arg1: 19
arg2: "hello 19"
hasNext: true
arg1: 20
arg2: "hello 20"
hasNext: true
arg1: 21
arg2: "hello 21"
hasNext: true
arg1: 22
arg2: "hello 22"
hasNext: true
arg1: 23
arg2: "hello 23"
hasNext: true
arg1: 24
arg2: "hello 24"
hasNext: true
arg1: 25
arg2: "hello 25"
hasNext: true
arg1: 26
arg2: "hello 26"
hasNext: true
arg1: 27
arg2: "hello 27"
hasNext: true
arg1: 28
arg2: "hello 28"
hasNext: true
arg1: 29
arg2: "hello 29"
hasNext: true
arg1: 30
arg2: "hello 30"
hasNext: true
arg1: 31
arg2: "hello 31"
hasNext: true
arg1: 32
arg2: "hello 32"
hasNext: true
arg1: 33
arg2: "hello 33"
hasNext: true
arg1: 34
arg2: "hello 34"
hasNext: true
arg1: 35
arg2: "hello 35"
hasNext: true
arg1: 36
arg2: "hello 36"
hasNext: true
arg1: 37
arg2: "hello 37"
hasNext: true
arg1: 38
arg2: "hello 38"
hasNext: true
arg1: 39
arg2: "hello 39"
hasNext: true
arg1: 40
arg2: "hello 40"
hasNext: true
arg1: 41
arg2: "hello 41"
hasNext: true
arg1: 42
arg2: "hello 42"
hasNext: true
arg1: 43
arg2: "hello 43"
hasNext: true
arg1: 44
arg2: "hello 44"
hasNext: true
arg1: 45
arg2: "hello 45"
hasNext: true
arg1: 46
arg2: "hello 46"
hasNext: true
arg1: 47
arg2: "hello 47"
hasNext: true
arg1: 48
arg2: "hello 48"
hasNext: true
arg1: 49
arg2: "hello 49"
hasNext: true
arg1: 50
arg2: "hello 50"
hasNext: true
arg1: 51
arg2: "hello 51"
hasNext: true
arg1: 52
arg2: "hello 52"
hasNext: true
arg1: 53
arg2: "hello 53"
hasNext: true
arg1: 54
arg2: "hello 54"
hasNext: true
arg1: 55
arg2: "hello 55"
hasNext: true
arg1: 56
arg2: "hello 56"
hasNext: true
arg1: 57
arg2: "hello 57"
hasNext: true
arg1: 58
arg2: "hello 58"
hasNext: true
arg1: 59
arg2: "hello 59"
hasNext: true
arg1: 60
arg2: "hello 60"
hasNext: true
arg1: 61
arg2: "hello 61"
hasNext: true
arg1: 62
arg2: "hello 62"
hasNext: true
arg1: 63
arg2: "hello 63"
hasNext: true
arg1: 64
arg2: "hello 64"
hasNext: true
arg1: 65
arg2: "hello 65"
hasNext: true
arg1: 66
arg2: "hello 66"
hasNext: true
arg1: 67
arg2: "hello 67"
hasNext: true
arg1: 68
arg2: "hello 68"
hasNext: true
arg1: 69
arg2: "hello 69"
hasNext: true
arg1: 70
arg2: "hello 70"
hasNext: true
arg1: 71
arg2: "hello 71"
hasNext: true
arg1: 72
arg2: "hello 72"
hasNext: true
arg1: 73
arg2: "hello 73"
hasNext: true
arg1: 74
arg2: "hello 74"
hasNext: true
arg1: 75
arg2: "hello 75"
hasNext: true
arg1: 76
arg2: "hello 76"
hasNext: true
arg1: 77
arg2: "hello 77"
hasNext: true
arg1: 78
arg2: "hello 78"
hasNext: true
arg1: 79
arg2: "hello 79"
hasNext: true
arg1: 80
arg2: "hello 80"
hasNext: true
arg1: 81
arg2: "hello 81"
hasNext: true
arg1: 82
arg2: "hello 82"
hasNext: true
arg1: 83
arg2: "hello 83"
hasNext: true
arg1: 84
arg2: "hello 84"
hasNext: true
arg1: 85
arg2: "hello 85"
hasNext: true
arg1: 86
arg2: "hello 86"
hasNext: true
arg1: 87
arg2: "hello 87"
hasNext: true
arg1: 88
arg2: "hello 88"
hasNext: true
arg1: 89
arg2: "hello 89"
hasNext: true
arg1: 90
arg2: "hello 90"
hasNext: true
arg1: 91
arg2: "hello 91"
hasNext: true
arg1: 92
arg2: "hello 92"
hasNext: true
arg1: 93
arg2: "hello 93"
hasNext: true
arg1: 94
arg2: "hello 94"
hasNext: true
arg1: 95
arg2: "hello 95"
hasNext: true
arg1: 96
arg2: "hello 96"
hasNext: true
arg1: 97
arg2: "hello 97"
hasNext: true
arg1: 98
arg2: "hello 98"
hasNext: true
arg1: 99
arg2: "hello 99"
hasNext: true
arg1: 100
arg2: "hello 100"
hasNext: true
arg1: 101
arg2: "hello 101"
hasNext: true
arg1: 102
arg2: "hello 102"
hasNext: true
arg1: 103
arg2: "hello 103"
hasNext: true
arg1: 104
arg2: "hello 104"
hasNext: true
arg1: 105
arg2: "hello 105"
hasNext: true
arg1: 106
arg2: "hello 106"
hasNext: true
arg1: 107
arg2: "hello 107"
hasNext: true
arg1: 108
arg2: "hello 108"
hasNext: false
arg1: 109
arg2: "hello 109"
```

--------------------------------------------
server reads:

```
type: MAP
transactionId: 1426526752786
timeStamp: 1426526752786
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
hasNext: true
arg1: 4
arg2: "hello 4"
hasNext: true
arg1: 5
arg2: "hello 5"
hasNext: true
arg1: 6
arg2: "hello 6"
hasNext: true
arg1: 7
arg2: "hello 7"
hasNext: true
arg1: 8
arg2: "hello 8"
hasNext: true
arg1: 9
arg2: "hello 9"
hasNext: true
arg1: 10
arg2: "hello 10"
hasNext: true
arg1: 11
arg2: "hello 11"
hasNext: true
arg1: 12
arg2: "hello 12"
hasNext: true
arg1: 13
arg2: "hello 13"
hasNext: true
arg1: 14
arg2: "hello 14"
hasNext: true
arg1: 15
arg2: "hello 15"
hasNext: true
arg1: 16
arg2: "hello 16"
hasNext: true
arg1: 17
arg2: "hello 17"
hasNext: true
arg1: 18
arg2: "hello 18"
hasNext: true
arg1: 19
arg2: "hello 19"
hasNext: true
arg1: 20
arg2: "hello 20"
hasNext: true
arg1: 21
arg2: "hello 21"
hasNext: true
arg1: 22
arg2: "hello 22"
hasNext: true
arg1: 23
arg2: "hello 23"
hasNext: true
arg1: 24
arg2: "hello 24"
hasNext: true
arg1: 25
arg2: "hello 25"
hasNext: true
arg1: 26
arg2: "hello 26"
hasNext: true
arg1: 27
arg2: "hello 27"
hasNext: true
arg1: 28
arg2: "hello 28"
hasNext: true
arg1: 29
arg2: "hello 29"
hasNext: true
arg1: 30
arg2: "hello 30"
hasNext: true
arg1: 31
arg2: "hello 31"
hasNext: true
arg1: 32
arg2: "hello 32"
hasNext: true
arg1: 33
arg2: "hello 33"
hasNext: true
arg1: 34
arg2: "hello 34"
hasNext: true
arg1: 35
arg2: "hello 35"
hasNext: true
arg1: 36
arg2: "hello 36"
hasNext: true
arg1: 37
arg2: "hello 37"
hasNext: true
arg1: 38
arg2: "hello 38"
hasNext: true
arg1: 39
arg2: "hello 39"
hasNext: true
arg1: 40
arg2: "hello 40"
hasNext: true
arg1: 41
arg2: "hello 41"
hasNext: true
arg1: 42
arg2: "hello 42"
hasNext: true
arg1: 43
arg2: "hello 43"
hasNext: true
arg1: 44
arg2: "hello 44"
hasNext: true
arg1: 45
arg2: "hello 45"
hasNext: true
arg1: 46
arg2: "hello 46"
hasNext: true
arg1: 47
arg2: "hello 47"
hasNext: true
arg1: 48
arg2: "hello 48"
hasNext: true
arg1: 49
arg2: "hello 49"
hasNext: true
arg1: 50
arg2: "hello 50"
hasNext: true
arg1: 51
arg2: "hello 51"
hasNext: true
arg1: 52
arg2: "hello 52"
hasNext: true
arg1: 53
arg2: "hello 53"
hasNext: true
arg1: 54
arg2: "hello 54"
hasNext: true
arg1: 55
arg2: "hello 55"
hasNext: true
arg1: 56
arg2: "hello 56"
hasNext: true
arg1: 57
arg2: "hello 57"
hasNext: true
arg1: 58
arg2: "hello 58"
hasNext: true
arg1: 59
arg2: "hello 59"
hasNext: true
arg1: 60
arg2: "hello 60"
hasNext: true
arg1: 61
arg2: "hello 61"
hasNext: true
arg1: 62
arg2: "hello 62"
hasNext: true
arg1: 63
arg2: "hello 63"
hasNext: true
arg1: 64
arg2: "hello 64"
hasNext: true
arg1: 65
arg2: "hello 65"
hasNext: true
arg1: 66
arg2: "hello 66"
hasNext: true
arg1: 67
arg2: "hello 67"
hasNext: true
arg1: 68
arg2: "hello 68"
hasNext: true
arg1: 69
arg2: "hello 69"
hasNext: true
arg1: 70
arg2: "hello 70"
hasNext: true
arg1: 71
arg2: "hello 71"
hasNext: true
arg1: 72
arg2: "hello 72"
hasNext: true
arg1: 73
arg2: "hello 73"
hasNext: true
arg1: 74
arg2: "hello 74"
hasNext: true
arg1: 75
arg2: "hello 75"
hasNext: true
arg1: 76
arg2: "hello 76"
hasNext: true
arg1: 77
arg2: "hello 77"
hasNext: true
arg1: 78
arg2: "hello 78"
hasNext: true
arg1: 79
arg2: "hello 79"
hasNext: true
arg1: 80
arg2: "hello 80"
hasNext: true
arg1: 81
arg2: "hello 81"
hasNext: true
arg1: 82
arg2: "hello 82"
hasNext: true
arg1: 83
arg2: "hello 83"
hasNext: true
arg1: 84
arg2: "hello 84"
hasNext: true
arg1: 85
arg2: "hello 85"
hasNext: true
arg1: 86
arg2: "hello 86"
hasNext: true
arg1: 87
arg2: "hello 87"
hasNext: true
arg1: 88
arg2: "hello 88"
hasNext: true
arg1: 89
arg2: "hello 89"
hasNext: true
arg1: 90
arg2: "hello 90"
hasNext: true
arg1: 91
arg2: "hello 91"
hasNext: true
arg1: 92
arg2: "hello 92"
hasNext: true
arg1: 93
arg2: "hello 93"
hasNext: true
arg1: 94
arg2: "hello 94"
hasNext: true
arg1: 95
arg2: "hello 95"
hasNext: true
arg1: 96
arg2: "hello 96"
hasNext: true
arg1: 97
arg2: "hello 97"
hasNext: true
arg1: 98
arg2: "hello 98"
hasNext: true
arg1: 99
arg2: "hello 99"
hasNext: true
arg1: 100
arg2: "hello 100"
hasNext: true
arg1: 101
arg2: "hello 101"
hasNext: true
arg1: 102
arg2: "hello 102"
hasNext: true
arg1: 103
arg2: "hello 103"
hasNext: true
arg1: 104
arg2: "hello 104"
hasNext: true
arg1: 105
arg2: "hello 105"
hasNext: true
arg1: 106
arg2: "hello 106"
hasNext: true
arg1: 107
arg2: "hello 107"
hasNext: true
arg1: 108
arg2: "hello 108"
hasNext: false
arg1: 109
arg2: "hello 109"
```

--------------------------------------------
server writes:

```
transactionId: 1426526752786
isException: false
```

--------------------------------
client read:

```
transactionId: 1426526752786
isException: false
```
