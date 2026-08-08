# Notes


## Questions you should be able to answer without notes

**Where are consumer offsets stored, and what happens to them if a group is idle for a week?**

Consumer offsets are stored in an internal, compacted Kafka topic named `__consumer_offsets`

Offset expiration is governed by the broker setting `offsets.retention.minutes`.
By default, this is set to 7 days (10,080 minutes). If a consumer group remains idle or inactive
beyond this retention window, Kafka deletes its committed offsets from `__consumer_offsets`.

When the group eventually restarts, Kafka treats it as a brand-new consumer group and relies on the
consumer's `auto.offset.reset` configuration (`earliest` or `latest`) to determine where to start reading.


**Why is increasing a topic's partition count a semi-breaking change for keyed data?**

Kafka assigns records with keys to partitions using a hashing function: 

$$\text{partition} = \text{hash}(\text{key}) \pmod{\text{total\_partitions}}$$

- **Loss of Ordering**: When you increase the partition count from $N$ to $M$, the modulo calculation changes. Future records produced with key "user_123" will hash to a different partition than historical records produced with the exact same key.
- **Impact on State**: Any stream processor, state store, or downstream service relying on per-key ordering or key-partition mapping (e.g., Kafka Streams KTables, joins) will experience broken ordering and fragmented local state across multiple partitions.


**What does a producer do when it has no key — and did that answer change in recent Kafka versions?**

- Pre-Kafka 2.4: The producer used a simple **round-robin** strategy. It cycled message-by-message across all available partitions, producing many partially filled batches, high CPU overhead, and increased latency.
- Kafka 2.4+ (Yes, this changed): Kafka introduced the Sticky Partitioner (KIP-480) as the default strategy. It "sticks" to a single partition and fills up a batch until either `batch.size` or `linger.ms is reached before picking another partition at random. This dramatically improves throughput, batch efficiency, and latency without altering user key distributions.

**Why can't you decrease the partition count of a topic?**

- Append-Only Architecture: Kafka **partition logs are immutable**, append-only files structured per-partition directory on disk.

**What is the difference between "the consumer received the record" and "the offset is committed"?**

- Consumer received the record and crashes right before committing the offset implies the redelivery of the same message once the consumer comes back up.
- Once the offset is committed such a message is only redelivered if and only if the partition's offset is reset.

**If a consumer group has 3 members and one hangs indefinitely without dying, what happens?**

- `session.timeout.ms` (Heartbeat mechanism): If the process stalls completely (e.g., long GC pause, process freeze) and stops sending heartbeats to the Group Coordinator, the coordinator marks the consumer dead after `session.timeout.ms`.
- `max.poll.interval.ms` (Processing mechanism): If the consumer's background heartbeat thread remains alive, but its application thread hangs (e.g., deadlock, infinite loop) and fails to call `poll()` before `max.poll.interval.ms` elapses, the consumer proactively leaves the group or is evicted.
