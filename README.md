What is this?
================

A crazy experiment to see if I can throw together a distributed data map with a few goals:

- Very simple API
  - It will only support a few operations to get and set data
  - Ability to expire data out of the map to a consistent store (synchronously)
- Strongly consistent data
  - Serializable isolation
  - Synchronous replication for failover consistency
- Code kept as simple, understandable, and maintainable as possible
- Mesh network clustering
  - Simple discovery mechanism
  - Support replications, failovers, rebalancing partitions

But will it blend?
=================

All of these are stretch goals and are probably crazy. I have no idea if I'll ever finish this.