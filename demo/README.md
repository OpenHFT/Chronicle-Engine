# Chronicle Demo.


# Purpose

Chronicle Engine supports a bootstrap configuration style.  
By "bootstrap configuration" we mean it specifies the class which loads the rest of the file and does the real work.
The benefit of this is your configuration files can change format and the way they are used over time and have this information in the configuration file itself.

## What is the benefit of using YAML configuration files.

- configuration includes aliased type information.  This supports easy extension through adding new classes/versions and cross platform through type aliasing.
- by supporting types, a configuration file can bootstrap itself. You control how the configuration file is decoded. [engine.yaml](https://github.com/OpenHFT/Chronicle-Engine/blob/master/demo/src/main/resources/engine.yaml)
- to send the configuration of a server to a client or visa-versa.
- in configuration be able to create any object or component as it supports object deserialization.
- save a configuration after you have changed it.

## To Run.

### Pre-requisites

Chronicle-Engine binaries are not published to maven central, so it is necessary to build locally from source.

To run the demo from the command-line, follow these steps:

```
# clone the repository
$ git clone https://github.com/OpenHFT/Chronicle-Engine.git
$ cd Chronicle-Engine

# install into local maven repository
$ mvn install

# run the demo
$ cd demo
$ mvn exec:java

```


Once the Chronicle-Engine project has been installed locally, you will be able to run the demo from your IDE.

Search for the RunEngineMain class, and execute it.

The source for the main is [RunEngineMain](https://github.com/OpenHFT/Chronicle-Engine/blob/master/demo/src/main/java/net/openhft/engine/chronicle/demo/RunEngineMain.java) which calls [EngineMain](https://github.com/OpenHFT/Chronicle-Engine/blob/master/src/main/java/net/openhft/chronicle/engine/EngineMain.java)

The configuration file is [engine.yaml](https://github.com/OpenHFT/Chronicle-Engine/blob/master/demo/src/main/resources/engine.yaml)