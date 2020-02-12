# Cloud Database / Group 4

The most awesome distributed cloud database.

## Requirements

* [Java 8](https://java.com/en/download/)
* [Apache Ant 1.10.x](https://ant.apache.org)

## Documentation

- [__Report__](report/G4 - Report.pdf)
- More detailed [architecture documentation](ARCHITECTURE.md).
- [Map/Reduce Tutorial](MAP_REDUCE.md)

### Building

Build and create JARs:

    ant clean build-jar
    
This will create 3 JARs:

* `ecs.jar`: External configuration service
* `server.jar`: KV server
* `client.jar`: Commandline client

### Usage

Start the external configuration service (ECS):

```
java -jar ecs.jar
```

Init the cluster with 5 nodes (cache of size 1000 and with FIFO strategy):

```
ECS Client> init 5 1000 FIFO
ECS Client> start
```

Add another node into the running cluster:

```
ECS Client> addNode 1000 FIFO
```

Remove an arbitrary node from the cluster:

```
ECS Client> removeNode
```

Shutdown all server nodes and exit the ECS:

```
ECS Client> shutDown
ECS Client> exit
```


## Contributing

We follow the official Java [coding style](https://www.oracle.com/technetwork/java/codeconvtoc-136057.html) as defined by Sun.
