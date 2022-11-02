What is this?
================

A crazy experiment to see if I can throw together a distributed data map with a few goals:

- Very simple API
  - It will only support a few operations to get and set data
  - Ability to expire data out of the map to a consistent store (synchronously)
- Safe and consistent data handling
  - Strongly consistent reads
  - Writes that completely succeed or completely fail - no acknowledged writes lost even in node failure
  - Synchronous replication for failover consistency ([Raft protocol](https://raft.github.io/))
- Code kept as simple, understandable, and maintainable as possible
  - Clear locking where necessary
  - Immutable data structures wherever possible
    - Data that doesn't change is safer and more understandable
    - Prefer on-heap memory to support frequent allocation and replacement of immutable structures
    - Let the garbage collector do what it does best
      - Be aware that G1 GC may not do well with [humongous object allocations](https://dzone.com/articles/whats-wrong-with-big-objects-in-java)
      - Possibly experiment with [ZGC](https://wiki.openjdk.org/display/zgc/Main) if pause times or large objects become problematic
- Mesh network clustering
  - Simple topology discovery mechanisms
  - Support replications, failovers, rebalancing partitions

But will it blend?
=====================

All of these are stretch goals and are probably crazy. I have no idea if I'll ever finish this.

Development phases
=====================

1. Basic Raft protocol
   - Mock networks for simplicity and ease of testing
   - Serialization work avoided with simple message classes for development
   - Tests to create latency, failure scenarios, debug and grow API
2. Basic state machine, fed by Raft protocol
    - A simple hash map, fed and maintained by Raft logs
3. Node communication over network
    - Serialization work, protocol, message design, connection durability, etc
4. Non-partitioned distributed cache
    - A simple single shard implementation
5. Partitioned distributed cache without the ability to repartition
    - An expansion of the above where sharding is now supported
    - Avoids any work to live repartition the data because it's complicated
6. Partitioned distributed cache with the ability to repartition
    - Complete the above with the ability to finally add and remove shards from the cluster
