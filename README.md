## locker - atomic distributed "check and set" for short-lived keys

`locker` is a distributed de-centralized consistent in-memory
key-value store written in Erlang. An entry expires after a certain
amount of time, unless the lease is extended. This makes it a good
practical option for locks, mutexes and leader election in a
distributed system.

In terms of the CAP theorem, `locker` chooses consistency by requiring
a quorum for every write. For reads, `locker` chooses availability and
always does a local read which can be inconsistent. Extensions of the
lease is used as an anti-entropy mechanism to eventually propagate all
leases.

It is designed to be used inside your application on the Erlang VM,
using the Erlang distribution to communicate with masters and
replicas.

Operations:

 * `locker:lock/2,3,4`
 * `locker:update/3,4`
 * `locker:extend_lease/3`
 * `locker:release/2,3`
 * `locker:wait_for/2`
 * `locker:wait_for_release/2`


### Writes

To achieve "atomic" updates, the write is done in two phases, voting and
commiting.

In the voting phase, the client asks every master node for a promise
that the node can later set the key. The promise is only granted if
the current value is what the client expects. The promise will block
any other clients from also receiving a promise for that key.

If the majority of the master nodes gives the client the promise
(quorum), the client can go ahead and commit the lock. If a positive
majority was not reached, the client will abort and delete any
promises it received.

### Reads

`locker` currently only offers dirty reads from the local node. If we
need consistent reads, a read quorum can be used.

### Failure

"So, this is all fine and good, but what happens when something
fails?". To make the implementation simple, there is a timeout on
every promise and every lock. If a promise is not converted into a
lock in time, it is simply deleted.

If the user process fails to extend the lease of its lock, the lock
expires without consulting any other node. If a node is partitioned
away from the rest of the cluster, the lock might expire too soon
resulting in reads returning the empty value. However, a new lock
cannot be created as a quorum cannot be reached.

Calling `locker:wait_for_release/2` will block until a lock expires,
either by manual release or from a expired lease.

### Lease expiration

Synchronized clocks is not required for correct expiration of a
lease. It is only required that the clocks progress at roughly the
same speed. When a lock is created or extended, the node will set the
expiration to `now() + lease_length`, which means that the user needs
to account for the skew when extending the lease. With leases in the
order of minutes, the skew should be very small.

When a lease is extended, it is replicated to the other nodes in the
cluster which will update their local copy if they don't already have
the key. This is used to bring new nodes in sync.

### Replication

A `locker` cluster consists of masters and replicas. The masters
participate in the quorum and accept writes from the clients. The
masters implements strong consistency. Periodically the masters send
off their transaction log to the replicas where it is replayed to
create the same state. Replication is thus asynchronous and reads on
the replicas might be inconsistent. Replication is done in batch to
improve performance by reducing the number of messages each replica
needs to handle. Calling `locker:wait_for/2` after a succesful write
will block until the key is replicated to the local node. If the local
node is a master, it will return immediately.

### Adding new nodes

New nodes may first be added as replicas to sync up before being
promoted to master. Every operation happening after the replica
joined, will be also propagated to the replica. The time to catch up
is then determined by how long it takes for all leases to be extended.

New nodes might also be set directly as masters, in which case the new
node might give negative votes in the quorum. As long as a quorum can
be reached, the out-of-sync master will still accept writes and catch
up as fast as a replica.

Using `locker:set_nodes/3` masters and replicas can be set across the
entire cluster in a "send-and-pray" operation. If something happens
during this operation, the locker cluster might be in an inconsistent
state.
