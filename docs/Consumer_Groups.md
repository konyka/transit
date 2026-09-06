# Consumer Groups over TCP

In-process `t_cgroup` already did O(1) round-robin. This note is the
design that put that picker on the client port without extra copies
on the hot path.

Goals, in order: **performance first**, then **fail closed**, then
**TDD**.

## Why a new frame

Trailing bytes on `OPEN` stay rejected. An old server must not
misread a group name as payload, and a new client must not depend
on an overloaded `OPEN`. `T_MSG_JOIN` is a rare control frame
(once per consumer). The data path is still one `PUSH`.

## Wire

See `docs/Wire_Protocol.md`. Three names, same charset as queues,
exact length (no trailing junk).

```
t_client_join_follow(client, group, consumer_id, queue, timeout_ms);
```

`join_follow` does a consumer `OPEN` first (required), then `JOIN`.
On `T_ERR_AGAIN` with a different client-port hint it redials once.
The lower-level pair is still `t_client_open_queue` + `t_client_join`
when the caller already waits on `ack_seq`. Both `join` and
`join_follow` are TCP-only; the in-process stub returns -1.

## Server model

One group per FIFO/priority queue. The first successful `JOIN`
names it; a later `JOIN` with a different group name is
`T_ERR_BUSY`. Broadcast queues refuse `JOIN` (`T_ERR_INVALID`):
they already fan out.

Membership:

| Condition | Result |
|-----------|--------|
| No consumer `OPEN` | `T_ERR_PERMISSION` |
| Duplicate consumer id | `T_ERR_EXISTS` |
| Same conn, same triple | `ACK` `T_OK` (idempotent) |
| Same conn, other id | `T_ERR_BUSY` |
| `CLOSE` / disconnect | member removed |

The first successful `JOIN` also writes the group name on the
queue (`t_queue_set_group`). That name stays until the queue is
deleted. A later `JOIN` with a different name is `T_ERR_BUSY`
even after every session has disconnected.

The live `t_cgroup` object stays while any session still has the
queue open. An empty group refuses dispatch: `server_flush_pull`
will not fall back to OPEN-only consumers. The message remains
pending. PUSH credits still gate each send.

When the last network `OPEN` on that queue is dropped, the live
group object is destroyed with the open-ref. The sticky name is
not. The next consumer `OPEN` or flush recreates an empty group
from the queue metadata so an outsider cannot steal. After a
Raft apply or snapshot restore the same restore runs. Clustered
first bind appends `T_RAFT_CMD_JOIN`. Unclustered durable queues
append a WAL `JOIN` record so restart keeps the name. Consumer ids
stay session-local on the server (clients re-`JOIN` after failover).
The TCP client remembers the last `JOIN` triple per queue and
`open_follow` replays it after a leader redial.

## Hot path

Unchanged consume-then-push:

1. `t_queue_consume` (one message)
2. `t_cgroup_pick` (O(1) RR, skip credit-exhausted sessions)
3. one `PUSH` + inflight slot

No payload copy through `t_cgroup_dispatch`. Pick returns the
session pointer stored as consumer `ud`.

Without a group, flush still picks the first OPEN consumer with
credit (existing competing-consumer behavior).

## TDD

1. `wire_join_roundtrip` / `wire_join_rejects_bad`
2. `cgroup_pick_round_robin` / empty pick is NULL
3. `server_join_requires_consumer_open`
4. `server_join_round_robin` — two members, two posts, 1+1
5. `server_join_duplicate_consumer` — EXISTS / BUSY / idempotent
6. `server_join_empty_holds` — outsider OPEN does not steal
7. `server_join_survives_last_close` — name survives last OPEN
8. `client_join_stub_fails`
9. `client_join_follow_to_leader` / `client_join_follow_no_hint_stays`
10. `queue_set_group_sticky` / `broker_raft_join_two_nodes`
11. `queue_durable_group_survives_reopen` / `broker_durable_group_roundtrip`
