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
t_client_open_queue(..., T_CLIENT_OPEN_CONSUMER);
t_client_join(client, group, consumer_id, queue);
```

`t_client_join` is TCP-only. The in-process stub returns -1.

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

The group object stays while any session still has the queue open.
An empty group refuses dispatch: `server_flush_pull` will not fall
back to OPEN-only consumers. The message remains pending. PUSH
credits still gate each send.

When the last network `OPEN` on that queue is dropped, the group
is destroyed with the open-ref.

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
7. `client_join_stub_fails`
