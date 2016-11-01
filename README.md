# Chronicle-Engine
A high performance, low latency, reactive processing framework

## Feature comparison
| Features                                           | Chronicle Engine | Chronicle Enterprise |
| ------------------------------------------ | ------------------ | ---------------------- |
| Hierarchical Data Organisation             | tree structure      | tree structure          |
| Remote access to Custom server side code | yes               | yes                       |
| Reactive Notification of changes           | yes                   | yes                        |
| Data View Virtualisation                      | yes                    | yes                       |
| Response time                                    | sub-milli-second | 10 micro-seconds   |
| Throughput Bandwidth/server (durable) | 100 MB/s           | 1 GB/s                  |
| Throughput Requests/sec/server (durable) | 1 M/s             | 10 M/s                  |
| Horizontal Scalability                           | 1 to 10              | 1 to 100                    |
| Data Access                                      | remote only         | Managed* remote   |
| LAN Replication                                 | TCP only           | Managed* TCP         |
| WAN Replication                                 | same as LAN      | Multi-cluster replication |
| User based access control                    | pluggable          | LDAP and Active Directory |
| Auditability of access                          | pluggable           | yes                       |
| Work distribution model                      | pluggable           | yes                       |
| Client side caching                              | pluggable          | yes                       |
| Monitoring Tools                                 | pluggable          | JMX & HTML5         |
| Pluggable backed data stores                | pluggable          | LDAP, JDBC, Camel |
| Cluster management (multi-server)        | pluggable          | yes                       |
| Code warmup on startup                      | no                     | yes                      |
| Dynamic code loading                         | no                     | yes                      |
| Distributed Locking                             | no                    | yes                       |
| C#, Excel client                                   | no                    | yes                      |
| C+ client                                            | no                    | planned                |

Managed* means compressed, encrypted, traffic shaping and centrally monitored.

## Features descriptions
### Hierarchical Data Organisation
The framework acts as a smart file system.

Each collections of data or services is an "Asset" on a tree.  Controls and configuration can be applied to any branch of a tree.  You can remotely access a branch of a tree from one machine to another.

```java
ConcurrentMap<String, String> map = acquireMap("/group1/sub-group/map", String.class, String.class);

TopicPublisher<String, String> pub = acquireTopicPublisher("/group1/sub-group/map", String.class, String.class);

pub.registerTopicSubscriber((k, v) -> System.out.println("key: " + k + ", value: " + v);

// these will trigger the topic subscriber to print;
// key: Hello, value: world
map.put("Hello", "world");
// or
pub.publish("Hello", "world");

String world = map.get("Hello");
```

### remote access to Custom Server side code.
Chronicle Engine supports serialization of lambdas as well as execution of predefined functions by name.

```java
// works on the server or the client.
String value = map.computeIfAbsent("hello", () -> "world");

Map<String, UUID> map2 = acquireMap("/group1/sub-group/uuids", String.class, UUID.class);
registerTopicSubscriber("/group1/sub-group/uuids", String.class, UUID.class, (k, uuid) -> System.out.println("key: " + k + ", uuid: " + uuid);

// for a new uuid the listener prints:
// key: user1, uuid: e2d9e382-2c98-4027-9019-8536fcee3225
UUID uuid = map2.computeIfAbsent("user1", UUID::randomUUID());
```

In this example a user's profile is updates and a listerner is subscribed to changes for that user.
```java
MapView<String, UserProfile> map3 = acquireMap("/group1/sub-group/users", String.class, UserProfile.class);

// subscribe to changes to jill's profile
Subscriber<UserProfile> jillSubscription = up -> System.out::println;
registerSubscriber("/group1/sub-group/users/jill", UserProfile.class, jillSubscriber);

// triggers an event which is sent to the jillSubscription
map3.asyncUpdate("jill", UserProfile::incrementUsage);

// doesn't triggers an event for jillSubscription
map3.asyncUpdate("bob", UserProfile::incrementUsage);

class UserProfile {
    long counter;

    public static UserProfile incrementUsage(UserProfile up) {
         if (up == null)
             up = new UserProfile();
         up.counter++;
         return up;
    }
}
```

### Reactive Notification of changes
As seen in the previous example, you can subscribe to change to a map.  If you need more details, you can subscribe to MapEvent(s)

```
Subscriber<TopologicalEvent> topSubscriber = e -> System.out.println("The tree has changed "+e);
registerSubscriber("/group1/sub-group",  TopologicalEvent.class, topSubscriber);

// If map is created dynamically this will trigger
// AddedAssetEvent{ assetName= '/group1/sub-group', name='map' }
ConcurrentMap<String, String> map = acquireMap("/group1/sub-group/map", String.class, String.class);

map.put("one", "won");

// print events as they happen
Subscriber<MapEvent> mapSubscriber = System.out::println;

// triggers a bootstrap of
// InsertedEvent{ assetName='/group1/sub-group/map', key= 'one', value='won' }
registerSubscriber("/group1/sub-group/map", MapEvent.class, mapSubscriber);

// triggers an event
// InsertedEvent{ assetName='/group1/sub-group/map', key= 'two', value='too' }
map.put("two", "too");

// triggers an event
// UpdateEvent{ assetName='/group1/sub-group/map', key= 'two', oldValue = 'too', value='to' }
map.put("two", "too");

// triggers an event
// RemovedEvent{ assetName='/group1/sub-group/map', key= 'one', oldValue = 'won' }
map.remove("one"));
```

### Data View Virtualisation
The same data store can can access in multiple ways to suit the use case of the Developer.  Say a user case is pub/sub but we want the latest value.

```
// setup the publisher for different topics.
TopicPublisher<String, String> pub = acquireTopicPublisher("/group1/sub-group/map", String.class, String.class);

// register a subscriber for any topic in a group on another machine.
registerTopicSubscriber("/group1/sub-group/map", String.class, String.class, (k, v) -> System.out.println("key: " + k + ", value: " + v);

// a third machine just wants to be able to see the latest value
ConcurrentMap<String, String> map = acquireMap("/group1/sub-group/map", String.class, String.class);

// these will trigger the topic subscriber to print;
// key: Hello, value: world
map.put("Hello", "world");
// or
pub.publish("Hello", "world");

// to get the latest value.
String world = map.get("Hello");
```

### Low latency response time.
Chronicle Engine which be design and tested to support sub-milli-second response times.  A single client should be able to achieve 10K requests per second with 99.9% of requests being under a milli-second.

Chronicle Enterprise will be tested to a higher spec and will have more heavily optimised components.  A single client should be able to achieve 100K requests per second with 99% of requests being under 100 micro-seconds.

### Code warmup on startup.
Chronicle Engine makes it easy to create a dummy engine to assist in warming up the code you will need in production.

Chronicle Enterprise can trigger the compilation of methods from a previous run e.g. in UAT.  Capture the output of -XX:+PrintCompilation, edit to taste, and use this file to trigger the compilation of these methods on start up.  This works for standard OpenJDK and Oracle JVMs.

```java
// load the methods compiled previously.
Warmup warmup = Warmup.compileFromFile(new File("print-compilation.txt")));
// print out the methods which will be compiled and to which level.
warmup.dump((m, l) -> System.out.println(m + " => " + l));
// start enqueuing the methods to be compiled in the background.
warmup.start();
// wait for those methods to be compiled.
warmup.waitFor();
```

If you have an object to navigate such as an AssetTree you can compile everything it uses and everything that uses etc.
```java
AssetTree tree = // tree of objects.

// load the methods compiled previously.
Warmup warmup = new Warmup();
// print out the methods which will be compiled and to which level.
warmup.compileForInstance(tree);
// start enqueuing the methods to be compiled in the background.
warmup.start();
// wait for those methods to be compiled.
warmup.waitFor();
```

### Chronicle Engine can be viewed on an NFS mount

Start the [Chronicle Engine Demo](https://github.com/OpenHFT/Chronicle-Engine/tree/master/demo)     
      
to mount :

First create the directory to which you want to mount
```
sudo mkdir /engine
```
####Then on Linux
```  
sudo mount -t nfs localhost:/ /engine
```  
NOTE: If you get this error:
```
mount: wrong fs type, bad option, bad superblock on localhost:/
```
You need to install nfs-common which you do as below:
```
apt-get install nfs-common
```
####Or if you are on a Mac OSX:
```
mount -o vers=4 localhost:/ /mnt
```
You should eject the mounted directory before killing the server.

####Or if you are on Windows:
You will need to an nfs4 client or use Microsoft service for unix and use nfsv3


the following example creates an entry containg key=hello value=world in the asset called /temp

```  
$cd /engine
$mkdir temp
cd temp
echo hello > world
```  
to unmount :
```  
$sudo umount /engine
```  


### In chronicle-Engine - you can call an arbitrary lambda or predefined function

for example

   List<Key> keys = ...
   List<Value> values = map.applyTo(m -> {
       List<Value> values = new ArrayList<>();
       for(Key k : keys)
            values.add(map.get(k));
       return values;
   });

Instead of using a lambda you can define an `enum` of predefined functions, see

https://github.com/OpenHFT/Chronicle-Engine/blob/master/src/main/java/net/openhft/chronicle/engine/map/remote/MapFunction.java

We also have a function for querying and/or updating either an individual
entry, or the map as whole remote.

Note: update is asynchronous by default so it acts like a pipelined
request. Some more are defined here

https://github.com/OpenHFT/Chronicle-Engine/blob/master/src/main/java/net/openhft/chronicle/engine/map/remote/MapUpdate.java

You can send a number of asynchronous calls and wait for a synchronous one.
A simple example is put which is asynchronous by default.

    map.put(key1, value1); // async
    map.put(key2, value2); // async
    map.put(key3, value3); // async
    map.size(); // sync

As size() is synchronous it must wait for the asynchronous calls to
complete before returning it's result.

   I think using benchmarks only over loop back is valid if you have a
business requirement to use loop back.  If your requirement is for two
processes to talk together, it is valid to compare the fastest method
available.

### Monitoring 

Chroncile engine, by default creates some system maps/queue that you can subsribe to get get an idea about whats going on in your system.

-       Users connected
-       Data flowing rates
-       Engine status

subscribing to the following URL's will give you some of this information, 

/proc/connections/cluster/throughput/" + localIdentifier  // Queue <NetworkStats>
/proc/connections/cluster/connectivity          //  Map<ConnectionDetails,ConnectionStatus)
/proc/connections/handlers                      //  Map<SocketChannel,TcpHandler)

### Question  

what kind of lambdas are possible, to run from the client onto the server ?  

#### Answer  

Any lambdas where the code exists both on the server and on the client. 
What you can’t do is create a custom lambda that just exists on the client.   


### Question   

In your example I see a lambda operating on N keys of the same type and returning N 
values of the same type.  What if I want to return a scalar or multiple scalars / keys or some other 
custom type I want to define?  

#### Answer  

This will work as long as the return result can be serialized, we support serialization  for all  
the primitive types, plus we also support the collections types such as Maps,Sets and Lists. 
 For custom types we recommend either extending the net.openhft.chronicle.wire.AbstractMarshallable 
or implementing Marshallable, ( this is our custom serilization approach, the code has  
been tuned to perform very well when using this, however if you wanted to consider using  
java.io.Serializable this should also work ) 
 
### Question 
 
What if I want to have a lambda [atomically] operate on multiple maps stored within 
the same  server?   
 
#### Answer 

You would have to implement your own locking strategy as atomicity across multiple maps is not 
 currently implemented.  

More details to come.


 


[![Alt text](https://img.youtube.com/vi/VID/0.jpg)](https://www.youtube.com/watch?v=VID)

