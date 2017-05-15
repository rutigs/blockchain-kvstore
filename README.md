#### Blockchain Distributed Key Value Store

This is a small concept multi-node key value store written in Go. It provides (bravely attempts to) the Atomicity, Consistency, and Independence in ACID. It uses blockchain technology via proof of work SHA256 hashing to integrate transactions into the store.

The system is able to continue working through multiple node failures.

#### KVService

KVService exposes the store's API to a client Go application.

#### KVNode

KVNode is run by all the nodes in the system. All the nodes are given a file of all other nodes ip:port to communicate to each other.

#### Malicious KVNode

Malicious KVNode is an attempt at recreating a sybil attack. When more than 50% of the nodes are malicious, the malicious nodes begin aborting real nodes transactions by creating and integrating conflicting transactions into the chain that will cause other transactions to abort.
