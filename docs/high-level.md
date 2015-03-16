All messages that are send to the server follow the initial header :


| field              |                           Description |
|:------------------ | -------------------------------------------------------------------------- |
|TYPE                |  This could be MAP, QUEUE, CORE >  |
|MAP | denotes that we wish to bind to a map instance,    |
|QUEUE  | denotes that we wish to bind to a map instance  |
|CORE | used when instruction the engine to carry out general tasks |


### transactionId: < a long number >
the transaction id must be a unique number of this request, it must be unique per server
connection. Typically this is implemented as a unique time stamp in milliseconds, but it is upto
 the client to decided who this is generated. The server will reflect the transaction id back to
 the client in response to a request. When chucking the server may provided several responses (
 within the same transaction id ) as it may not be possible for all the data to fit within a
 single TcpBuffer.

### timeStamp: 1426502826520
The time stamp in milliseconds that the client sent the response, this field should always be
sent in its entirety and should not be derived from the transaction-id is as an offset, as the
transaction id may not be necessary be a time stamp.

### channelId: 1
currently implemented as a number, but this will short changed to a String, this string will
become the server name.

----------------------------------

#### Example of the above
```
type: MAP
transactionId: 1426502826520
timeStamp: 1426502826520
channelId: 1
```

#### Spec

1. engine provides a service interface, these services maybe MAP's or QUEUE's  ( amongst others )

2. On initial connection the server and the client version numbers :
in later

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


the server notifies that it recieved this message by relecting the transaction id back


server writes:
```
transactionId: 1426504502494
isException: false


not all all requests require a require a reply, but some do. receiving a reply with a

isException: false

gives you confidence that the request was processed correctly. For messages that have

isException: true

the subsequest result will contain an exception :

result : < the exception as a text string>



