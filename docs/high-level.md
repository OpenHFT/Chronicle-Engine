# Engine High Level Spec

## Header
All messages that are sent from a client to a Chronicle Engine via TCP contain a header, below is
 the specification for this header.


#### Type: <see table below >

| type               | description   |
|:------------------ | --------------------------------------------------------------------------  |
|Map    | denotes that we wish to bind to a map instance,                                          |
|Queue  | denotes that we wish to bind to a map instance                                           |
|Core   | used when instruction the engine to carry out general tasks                              |


#### Transaction Id: < a long number >
the transaction id must be a unique number of this request, it must be unique per server
connection. Typically this is implemented as a unique time stamp in milliseconds, but it is upto
 the client to decided who this is generated. The server will reflect the transaction id back to
 the client in response to a request. When chucking the server may provided several responses (
 within the same transaction id ) as it may not be possible for all the data to fit within a
 single TcpBuffer.

#### Time Stamp: < time stamp in milliseconds ( EPOC ) >
The time stamp in milliseconds that the client sent the response, this field should always be
sent in its entirety and should not be derived from the transaction-id is as an offset, as the
transaction id may not be necessary be a time stamp.

#### Channel Id: <unique channel id>
Currently implemented as a number, but this will short changed to a String, this string will
become the server name.

----------------------------------

##### Example of the above

```
type: MAP
transactionId: 1426502826520
timeStamp: 1426502826520
channelId: 1
```

#### Overview

1. Engine provides a service interface, these services maybe MAP's or QUEUE's  ( amongst others )

2. On initial connection the server and the client version numbers :

If the version number differ
- A warning is logged.
- Data can only be exchanged using text wire, NOT binary wire.

3. in later version the services will be given names, in the current version under the covers
this name maps to a channel id. this channel id must be unique mapped to the service name.

Each request has a transaction :

client writes:
```
type: MAP
transactionId: 1426504502494
timeStamp: 1426504502494
```

the server notifies that it recieved this message by relecting the transaction id back


server writes:
```
transactionId: 1426504502494
isException: false
```

not all all requests require a require a reply, but some do. receiving a reply with a
 ```
isException: false
```
gives you confidence that the request was processed correctly. For messages that have
```
isException: true
```

the subsequent result will contain an exception :
```
result : < the exception as a text string>
```

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


