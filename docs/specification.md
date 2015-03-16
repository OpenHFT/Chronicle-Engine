## Chronicle Engine - TCP Client/Server Remote Interprocess communication using Chronicle Wire

This page covers the specification of the initial hand shaking and header for Chronicle Engine TCP connectivity, if you wish to look at the message flow based on, some chronicle map functions, see the table below :

| method   | description      |   URL                                                                                         |
|:---------|------|-----------------------------------------------------------------------------------------------------------|
|toString()|                  |    https://github.com/OpenHFT/Chronicle-Engine/blob/master/docs/examples/toString.md          |
|toString()|large number of entries|https://github.com/OpenHFT/Chronicle-Engine/blob/master/docs/examples/toString-large-number-of-entries.md|           
|put<key,value>|              | https://github.com/OpenHFT/Chronicle-Engine/blob/master/docs/examples/put.md                  |
|putAll<map>|           | https://github.com/OpenHFT/Chronicle-Engine/blob/master/docs/examples/putAll.md                  |
|get<key>  | returns non null |https://github.com/OpenHFT/Chronicle-Engine/blob/master/docs/examples/get-returns-a-value.md   |
|get<key>  | returns null     |https://github.com/OpenHFT/Chronicle-Engine/blob/master/docs/examples/get-returns-null.md      |


## Header
All messages that are sent from a client to a Chronicle Engine via TCP contain a header, below is
 the specification for this header.


#### Type: <see table below >

| type  | description   |
|:----- | --------------------------------------------------------------------------  |
|Map    | denotes that we wish to bind to a map instance.                                          |
|Queue  | denotes that we wish to bind to a queue instance. |
|Core   | denotes that we wish to carry out general tasks,for example, changing the wire format.                              |


#### Transaction Id: < a long number >
The transaction id must be a unique number of this request, it must be unique per server
connection. Typically this is implemented as a unique time stamp in milliseconds, but it is upto
 the client to decide how this is generated. The server will reflect the transaction id back to
 the client in response to a request. When responding the server may provide several
 responses or entries for a single transaction id, this is typically how entry set works. How
 ever if the number of entries are large and they don’t all fit into a single tcp/ip buffer the server may send additional
 data with the same transaction id in order to complete the message.

#### Time Stamp: < time stamp in milliseconds ( epoc ) >
A time stamp in milliseconds. This is the time the client sent the request.
This time is used by the maps reconciliation algorithm, so ideally should be as accurate as
possible, this field should always be sent in its entirety and should not be derived from the
transaction id is as an offset, as the transaction-id may not necessary be a time stamp.

#### Channel Id: <unique channel id>
Currently implemented as a number, but this will shortly changed to a String, this String will
become the service name.

----------------------------------

## Example Header

```
type: MAP
transactionId: 1426502826520
timeStamp: 1426502826520
channelId: 1
```

#### Overview


1. Engine provides a service interface, these services maybe MAP's or QUEUE's  ( amongst others )
2. On initial connection the server and the client version numbers are exchanged :
If the version number differs between the client and server,  A warning is logged, Data can only be exchanged using text wire, NOT binary wire. Otherwise, If the version numbers are the same between the client and server:
the client can ask the server to change to use the more efficient binary wire encoding.

3. In later versions the services will be given names, in the current version under the covers
this name maps to a channel id. this channel id must be unique mapped to the service name.

4. Each request has a transaction, the server will in most cases ( accept for a few rare cases ) reflect this transaction id to the client. If the client does not receive the transaction id it should time out the message to the user.
The client can handle a number of simultaneous requests ( each on a different thread ). A single TCP connection is used to handle all the traffic between the client and the server, and the thread that is making a request does not hold onto the socket connection waiting for a response. It will allow other threads in the meantime, to make separate requests, once a response is received it should marry up with the thread that made the request and hence return the result back to the user. So from a users perspective each thread blocks until it gets a response but you can make a number of different request at the same time, as long as they are on separate threads they don’t block each other.


## Example message :

client writes:
```
type: MAP
transactionId: 1426504502494
timeStamp: 1426504502494
```

the server notifies that it relieved this message by reflecting the transaction id back


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

##  Detailed Specification

##### Below is covered some of the points raised above in more detail

When clients connect to the server, they should exchange version numbers, below show sample message
 of this exchange :

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

Engine supports the following API=

``` java
public interface ChronicleContext {
    ...

    <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass);

    <E> ChronicleSet<E> getSet(String name, Class<E> eClass);

    <I> I getService(Class<I> iClass, String name, Class... args);

    ...
}
```

This lets you access a map, queue or other service by name, currently the client translates that
name into a channel id ( which at the moment is a number ) however later versions will support a
channel ID as a name.
```
 channelId: 1
```
and in later versions
```
 channelId: <name of channel>
```

If you wished to create a Map that has

```
key -> Integer
value -> String
```

when the name of the map is resolved to channel1 then the protocol becomes the following :

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
}
```
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
```

The key and value types are sent as the fully qualified name of the classes.
channel 1 is reserved for holding the details about the maps, in other words the service
descriptor is held in a map at channel 1, this descriptor holds the keys class value class and
channel id. } in this example above the name of the service was "test", it was created from the
following java code :

``` java
RemoteTcpClientChronicleContext context = new RemoteTcpClientChronicleContext(
       "localhost", 1234);

ChronicleMap<Integer, String> map = context.getMap("test", Integer.class, String.class);

map.put(1, "hello");
```

in this example the server is connecting to a remote map on localhost:1234, but this would be
changed to your host and port. Now the client is able to write data the this new service, in
other words channel 2, so as in the code sample above we are going to

``` java
map.put(1, "hello");
```

and the following data is exchanged between the client and server :

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
```
given that this is a new map, the map was empty when put added data to it, hence returning the
old value will return null.

resultIsNull: true


