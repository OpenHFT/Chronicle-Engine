# Getting data from map

on initial connection the server and the client exchange version numbers, see example below :
```
--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426502826520
timeStamp: 1426502826520
channelId: 1
methodName: applicationVersion
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426502826520
timeStamp: 1426502826520
channelId: 1
methodName: applicationVersion
```
--------------------------------------------
server writes:
```
transactionId: 1426502826520
isException: false
result: 3.0.0-alpha-SNAPSHOT

```
--------------------------------
client reads:
```
transactionId: 1426502826520
isException: false
result: 3.0.0-alpha-SNAPSHOT
```

If the version number differ
- A warning is logged.
- Data can only be exchanged using text wire, NOT binary wire.


# Service API

engine uses the following API to :

```
public interface ChronicleContext {

    ...

    <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass);

    <E> ChronicleSet<E> getSet(String name, Class<E> eClass);

    <I> I getService(Class<I> iClass, String name, Class... args);

    ...
}
```

This lets you access a map, queue of other serve by name, maps that name to a channel ID, for
example :

 channelId: 1

how ever later version will support channel names, for example

 channelId: <name of channel>


If you wished to create a Map that has

key -> Integer
value -> String

and the map name of the map is resolved to channel1 then the protocol becomes the following :


client writes:
```
type: MAP
transactionId: 1426504502494
timeStamp: 1426504502494
channelId: 1
methodName: createChannel
arg1: 2
```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426504502494
timeStamp: 1426504502494
channelId: 1
methodName: createChannel
arg1: 2
```
--------------------------------------------
server writes:
```
transactionId: 1426504502494
isException: false

```
--------------------------------
client read:
```
transactionId: 1426504502494
isException: false

```
--------------------------------------------
client writes:
```
type: MAP
transactionId: 1426504502816
timeStamp: 1426504502816
channelId: 1
methodName: put
arg1: test
arg2: { keyClass: java.lang.Integer
valueClass: java.lang.String
channelID: 2
}

```
--------------------------------------------
server reads:
```
type: MAP
transactionId: 1426504502816
timeStamp: 1426504502816
channelId: 1
methodName: put
arg1: test
arg2: { keyClass: java.lang.Integer
valueClass: java.lang.String
channelID: 2
}```
--------------------------------------------
server writes:
```
transactionId: 1426504502816
isException: false
resultIsNull: true

```
--------------------------------
client read:
```
transactionId: 1426504502816
isException: false
resultIsNull: true

The key and value types are sent as the fully qualified name of the classes.
channel 1 is reserved for holding the details about the maps, in other word the service
descriptor is held in a map at channel 1, this descriptor holds the keys class value class and
channel id. } in this example above the name of the service was "test", it was created from the
following java code :

```
RemoteTcpClientChronicleContext context = new RemoteTcpClientChronicleContext(
       "localhost", 1234);

ChronicleMap<Integer, String> map = context.getMap("test", Integer.class, String.class);

map.put(1, "hello");
```

in this example the server is connecting to a remove map on localhost:1234, but this would be
changed to you host and port.

now the client is able to write data the this new service, in other words channel 2, so as in the
 code sample above we are going to map.put(1, "hello");

```
------------------------------
client writes:

```
type: MAP
transactionId: 1426504502823
timeStamp: 1426504502823
channelId: 2
methodName: put
arg1: 1
arg2: hello
```
-------------------------------
server reads:

```
type: MAP
transactionId: 1426504502823
timeStamp: 1426504502823
channelId: 2
methodName: put
arg1: 1
arg2: hello
```
--------------------------------
server writes:
```
transactionId: 1426504502823
isException: false
resultIsNull: true

```
--------------------------------
client read:
```
transactionId: 1426504502823
isException: false
resultIsNull: true

given that this is a new map, the map was empty when put added data to it, hence returning the
old value will return null.

resultIsNull: true













