## locker

The goal of `locker` is to provide an atomic distributed "check and set"
operation for short-lived locks.

`locker` is a distributed de-centralized consistent in-memory
key-value store written in Erlang. An entry expires after a certain
amount of time, unless the lease is extended. This makes it a good
practical option for locks, mutexes and leader election in a
distributed system.

In terms of the CAP theorem, `locker` by default chooses consistency
by requiring a quorum for every write. Leases expiring during a
netsplit, can cause reads to to be inconsistent.

There are three operations:

 * `locker:lock/2,3`
 * `locker:extend_lease/3`
 * `locker:release/2`


### Writes

To achieve atomic updates, the write is done in two phases, voting and
commiting.

In the voting phase, the user calls every node and asks for a promise
that the node can later set the key. This promise will block any other
user from also receiving a promise for that key.

If the majority of the nodes gives the user the promise (quorum), the
user can go ahead and commit the lock. If a positive majority was not
reached, the user will abort and delete any promises it received.

### Node failure

"So, this is all fine and good, but what happens when a node
fails?". To make the system simple to implement, there is a timeout on
every promise and every lock. If a promise is not converted into a
lock in time, it is simply deleted.

If the user process fails to extend the lease of it's lock, the lock
expires without consulting any other node. If a node is partitioned
away from the rest of the cluster, the lock might expire too soon
resulting in reads returning the empty value. However, a new lock
cannot be created as a quorum cannot be reached.

In the future, subscribers to a lock could be notified when it expires.


### Lease expiration

Synchronized clocks is not required for correct expiration of a
lease. It is only required that the clocks progress at roughly the
same speed. When a lock is created or extended, the node will set the
expiration to `now() + lease_length`, which means that the user needs
to account for the skew when extending the lease. With leases in the
order of minutes, the skew should be very small.

When a lease is extended, it is gossiped to the other nodes in the
cluster which will update their local copy if they don't already have
the key. This is used to bring new nodes in sync.


### Adding new nodes

The new node needs to be made aware of the current cluster, by calling
`locker:add_node/1` for every other node in the cluster. This will
also add the reverse connection. The Erlang distribution is used, so
the cluster needs to already be connected or to connect without
authentication.

The new node will immediately start participating in writes. It will
return empty for keys where the lease has not yet been extended,
eventually catching up and returning identical responses to the other
nodes.


#### Assumptions

 * Two or more datacenters, latency at minimum 5ms between nodes
